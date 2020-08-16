package org.tomitribe.activemq.util;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.junit.Assert;
import org.junit.Test;
import org.tomitribe.util.Files;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class KahaDbUtilTest {

    @Test
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


}