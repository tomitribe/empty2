/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tomitribe.activemq.util;

import java.io.File;
import java.io.IOException;
import java.lang.IllegalStateException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaEntryType;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.util.DataByteArrayInputStream;
import org.apache.activemq.util.ByteSequence;
import org.tomitribe.crest.api.Command;
import org.tomitribe.crest.api.Option;
import org.tomitribe.crest.api.Required;
import org.tomitribe.util.IO;

public class KahaDbUtil {

    private static final Logger log = Logger.getLogger(KahaDbUtil.class.getName());

    @Command("display")
    public void display(
            final @Option("kahaDB") @Required File kahaDB) throws Throwable {

        process(kahaDB, (destination, message) -> log.info(destination.toString() + ": " + message.toString()));
    }

    @Command("migrate")
    public void migrate(
            final @Option("username") String username,
            final @Option("password") String password,
            final @Option("brokerURL") @Required String brokerURL,
            final @Option("kahaDB") @Required File kahaDB) throws Throwable {

        final ConnectionFactory remoteCf = new ActiveMQConnectionFactory(username, password, brokerURL);
        final Connection remoteConn = remoteCf.createConnection();
        remoteConn.start();

        final Session remoteSession = remoteConn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        process(kahaDB, (destination, message) -> {
            try {
                final MessageProducer producer = remoteSession.createProducer(destination);
                producer.send(message);
                producer.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });

        remoteSession.close();
        remoteConn.close();
    }

    void process(final File kahaDB, final BiConsumer<Destination, Message> messageConsumer) throws Exception {
        File kahaDBFolder;

        if (! kahaDB.exists()) {
            throw new IllegalStateException("KahaDB " + kahaDB.getAbsolutePath() + " does not exist");
        }

        if (kahaDB.isDirectory()) {
            kahaDBFolder = kahaDB;
        } else {
            final File tempFile = File.createTempFile("kahadb", "tmp");
            tempFile.delete();
            tempFile.mkdirs();
            kahaDBFolder = tempFile;

            IO.copy(kahaDB, new File(kahaDBFolder, kahaDB.getName()));
        }

        final BrokerService broker = new BrokerService();
        broker.setUseJmx(false);

        final PersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(kahaDBFolder);
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.start();

        final ConnectionFactory localConnectionFactory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
        final Connection localConnection = localConnectionFactory.createConnection();
        localConnection.start();
        final Session localSession = localConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        for (final Destination destination : broker.getBroker().getDestinations()) {
            if (destination instanceof Queue && !broker.checkQueueSize(((Queue) destination).getQueueName())) {
                log.info(String.format("Processing messages for '%s'...", destination.toString()));
                long migratedMessageCount = 0;
                final MessageConsumer localConsumer = localSession.createConsumer(destination, "");
                Message message = null;
                do {
                    message = localConsumer.receive(1000L);
                    if (message != null) {
                        messageConsumer.accept(destination, message);
                        message.acknowledge();
                        ++migratedMessageCount;
                    }
                } while (message != null || !broker.checkQueueSize(((Queue) destination).getQueueName()));
                localConsumer.close();
                log.info(String.format("Finished processing %s messages for '%s'.", migratedMessageCount, destination.toString()));
            }
        }

        localSession.close();
        localConnection.close();

        broker.stop();
    }

    @Command("find-unconsumed-messages")
    public DatabaseInfo findUnconsumedMessages(
            final @Option("kahaDB") @Required File kahaDB) throws Exception {

        final Map<String, MessageInfo> unconsumedMessages = new HashMap<>();

        int journalSize = getJournalSize(kahaDB);
        final Journal journal = createJournal(kahaDB, journalSize);

        log.info("Starting journal...");
        journal.start();

        log.info("Reading journal...");
        int fileIndex = 0;
        int dataIndex = 0;
        File lastFile = null;
        int messages = 0;

        Location location = journal.getNextLocation(null);
        while (location != null) {
            File nextFile = journal.getFile(location.getDataFileId());
            if(lastFile == null || !lastFile.equals(nextFile)) {

                lastFile = nextFile;
                dataIndex = 1;
                ++fileIndex;

                log.info("Reading new journal file: " + fileIndex);
            }
            else {
                ++dataIndex;
            }

            ByteSequence sequence = journal.read(location);
            DataByteArrayInputStream sequenceDataStream = new DataByteArrayInputStream(sequence);
            KahaEntryType commandType = KahaEntryType.valueOf(sequenceDataStream.readByte());

            log.finest("Command type " + commandType.toString() + " found at location " + location);

            if (KahaEntryType.KAHA_ADD_MESSAGE_COMMAND.equals(commandType)) {
                final KahaAddMessageCommand addMessageCommand = (KahaAddMessageCommand) commandType.createMessage().mergeFramed(sequenceDataStream);
                final String destination = addMessageCommand.getDestination().toString();
                final String messageId = addMessageCommand.getMessageId();
                unconsumedMessages.put(messageId, new MessageInfo(destination, messageId, location));
                messages++;
            }
            if (KahaEntryType.KAHA_REMOVE_MESSAGE_COMMAND.equals(commandType)) {
                final KahaRemoveMessageCommand removeMessageCommand = (KahaRemoveMessageCommand) commandType.createMessage().mergeFramed(sequenceDataStream);
                final String messageId = removeMessageCommand.getMessageId();
                unconsumedMessages.remove(messageId);
            }

            location = journal.getNextLocation(location);
        }

        log.info("Messages: " + messages);
        log.info("Unconsumed messages: " + unconsumedMessages.size());
        log.info("Closing journal...");
        journal.close();

        return new DatabaseInfo(messages, unconsumedMessages);
    }

    // @Command - browser

    public static Journal createJournal(File directory, int journalSize) {
        Journal result = new Journal();

        result.setDirectory(directory);
        result.setMaxFileLength(journalSize);
        result.setCheckForCorruptionOnStartup(false);
        result.setChecksum(false);
        result.setWriteBatchSize(Journal.DEFAULT_MAX_WRITE_BATCH_SIZE);
        result.setArchiveDataLogs(false);

        return result;
    }

    public static int getJournalSize(File directory) {
        Journal journal = new Journal();
        journal.setDirectory(directory);

        int journalSize = Journal.DEFAULT_MAX_FILE_LENGTH;
        try {
            journal.start();
            Location location = journal.getNextLocation(null);
            if(location != null) {
                journalSize = (int)journal.getFile(location.getDataFileId()).length();
            }
        }
        catch (Throwable throwable) {
        }
        finally {
            try {
                journal.close();
            } catch (IOException e) {  }
        }

        return journalSize;
    }

}

