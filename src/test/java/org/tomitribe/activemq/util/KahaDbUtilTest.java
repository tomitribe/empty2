package org.tomitribe.activemq.util;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.tomitribe.util.Files;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class KahaDbUtilTest {

    @Test
    @Ignore
    public void testSample() throws Exception {
        final File kahadb = Files.tmpdir(true);
        final BrokerService broker = new BrokerService();
        broker.setUseJmx(false);

        final PersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(kahadb);
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.start();

        {
            final ConnectionFactory cf = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
            final Connection conn = cf.createConnection();
            conn.start();

            final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            final Queue queue = session.createQueue("TESTQ");
            final MessageProducer producer = session.createProducer(queue);
            final TextMessage message = session.createTextMessage("Test message");
            message.setIntProperty("messageno", 1);
            producer.send(message);
            producer.close();
            session.close();
            conn.close();
        }

        broker.stop();

        final Map<String, List<String>> results = new HashMap<>();

        new KahaDbUtil().process(kahadb, new BiConsumer<Destination, Message>() {
            @Override
            public void accept(Destination destination, Message message) {
                try {
                    final List<String> messages = results.computeIfAbsent(destination.toString(), s -> new ArrayList<>());
                    messages.add(TextMessage.class.cast(message).getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        Assert.assertEquals(1, results.keySet().size());
        final List<String> messages = results.get("queue://TESTQ");

        Assert.assertEquals(1, messages.size());
        Assert.assertEquals("Test message", messages.get(0));
    }


    @Test
    public void testFindUnconsumedMessagesInASingleQueue() throws Exception {
        final File kahadb = Files.tmpdir(true);

        // create a broker
        final BrokerService broker = createBroker(kahadb);

        // produce 10 messages for queue queue://TEST
        produceMessagesOnQueue(broker.getVmConnectorURI().toString(), "TEST", 10);

        // consume 10 message from queue://TEST
        consumeMessagesFromQueue(broker.getVmConnectorURI().toString(), "TEST", 5);

        stopBroker(broker);

        // run the KahaDBUtil method

        final DatabaseInfo databaseInfo = new KahaDbUtil().findUnconsumedMessages(kahadb);

        // check that we have a total of 10 messages
        Assert.assertEquals(10, databaseInfo.getTotalMessageCount());

        // check that there are 5 unconsumed messages
        Assert.assertEquals(5, databaseInfo.getUnconsumedMessages().size());

        // check the location file IDs are all 1
        databaseInfo.getUnconsumedMessages().values().forEach(m -> Assert.assertEquals(1, m.getLocation().getDataFileId()));

        // check that the location offsets are all different
        final Set<Integer> offsets = databaseInfo.getUnconsumedMessages().values().stream()
                .map(m -> m.getLocation().getOffset())
                .collect(Collectors.toSet());

        Assert.assertEquals(5, offsets.size());
    }

    private void consumeMessagesFromQueue(final String brokerUrl, final String queueName, final int numberOfMessages) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        final Connection conn = cf.createConnection();
        conn.start();

        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Queue queue = session.createQueue(queueName);
        final MessageConsumer consumer = session.createConsumer(queue);

        for (int i = 0; i < numberOfMessages; i++) {
            final Message receivedMessage = consumer.receive(1000);
            Assert.assertNotNull(receivedMessage);
        }

        consumer.close();
        session.close();
        conn.close();
    }

    private void produceMessagesOnQueue(final String brokerUrl, final String queueName, final int numberOfMessages) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        final Connection conn = cf.createConnection();
        conn.start();

        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Queue queue = session.createQueue(queueName);
        final MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < numberOfMessages; i++) {
            final TextMessage message = session.createTextMessage("Test message: " + i);
            message.setIntProperty("messageno", i);
            producer.send(message);
        }

        producer.close();
        session.close();
        conn.close();
    }

    private void stopBroker(BrokerService broker) throws Exception {
        broker.stop();
    }

    private BrokerService createBroker(final File kahadb) throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setUseJmx(false);

        final PersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(kahadb);
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.start();

        return broker;
    }


    public void testCreateAReallyTerribleKahaDB() throws Exception {
        // use 1MB log files, 2 queues

        // add 10x 1k messages to queue 1
        // add 1500 1k messages to queue 2

        // the journal should have rolled over
        // repeat
        // we should rollover again

        // consume everything from queue 2
        // we now have 20x 1k messages occupying a the whole db
        // the db should have grown (not shrunk) following the consumption

        // run the test to find the unconsumed messages
        // assert that we get the right data
    }

    // similar test with topics and durable subscribers

    // similar test with unfinished transactions

    // similar with remove destination

    // similar with prepare/rollback

}