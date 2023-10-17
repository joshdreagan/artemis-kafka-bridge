package org.apache.activemq.artemis.akb;

import org.apache.activemq.artemis.akb.kafka.MockClientFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQExtension;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.awaitility.Awaitility.*;
import static org.hamcrest.Matchers.*;

@SuppressWarnings("unused")
public class ArtemisKafkaBrokerPluginTest {

  private static final Logger log = LoggerFactory.getLogger(ArtemisKafkaBrokerPluginTest.class);
  
  public static final String ADDRESS = "app.foo";
  public static final String ADDRESS_INBOUND = "app.foo.inbound";

  @RegisterExtension
  private static EmbeddedActiveMQExtension artemisServer = new EmbeddedActiveMQExtension("broker.xml");

  private static MockClientFactory kafkaClientFactory = new MockClientFactory();
  private static AtomicLong kafkaConsumerOffset = new AtomicLong(0);

  private MockProducer<byte[], byte[]> kafkaProducer;
  private MockConsumer<byte[], byte[]> kafkaConsumer;

  @BeforeEach
  void logBeforeEach(TestInfo testInfo) {
    String name = testInfo.getDisplayName();
    if (name == null || name.isBlank()) {
      name = testInfo.getTestMethod().get().getName();
    }
    log.info("Starting test '{}'.", name);
  }

  @AfterEach
  void logAfterEach(TestInfo testInfo) {
    String name = testInfo.getDisplayName();
    if (name == null || name.isBlank()) {
      name = testInfo.getTestMethod().get().getName();
    }
    log.info("Finished test '{}'.", name);
  }
  
  @BeforeEach
  void beforeEach() throws Exception {
    kafkaProducer = kafkaClientFactory.createKafkaProducer();
    kafkaConsumer = kafkaClientFactory.createKafkaConsumer();
  }

  @AfterEach
  void afterEach() throws Exception {
    kafkaProducer.clear();
  }

  @Test
  @DisplayName("testArtemisProducer")
  void testArtemisProducer() throws Exception {
    artemisServer.sendMessage(ADDRESS, "Holy crap it works!!!");
    await().atMost(5L, TimeUnit.SECONDS).until(() -> kafkaProducer.history().size(), equalTo(1));
  }

  @Test
  void testArtemisConsumer() throws Exception {
    kafkaConsumer.schedulePollTask(() -> {
      kafkaConsumer.rebalance(Collections.singletonList(new TopicPartition(ADDRESS, 0)));
      ConsumerRecord kafkaRecord = new ConsumerRecord(ADDRESS, 0, kafkaConsumerOffset.getAndIncrement(), null, "Holy crap it works!!!".getBytes(StandardCharsets.UTF_8));
      kafkaRecord.headers().add("AkbMessageId", "12345".getBytes(StandardCharsets.UTF_8));
      kafkaRecord.headers().add("AkbDestinationName", ADDRESS.getBytes(StandardCharsets.UTF_8));
      kafkaRecord.headers().add("AkbRoutingType", "ANYCAST".getBytes(StandardCharsets.UTF_8));
      kafkaConsumer.addRecord(kafkaRecord);
    });

    Map<TopicPartition, Long> startOffsets = new HashMap<>();
    TopicPartition tp = new TopicPartition(ADDRESS, 0);
    startOffsets.put(tp, kafkaConsumerOffset.get());
    kafkaConsumer.updateBeginningOffsets(startOffsets);

    artemisServer.setDefaultReceiveTimeout(30000L);
    ClientMessage artemisMessage = artemisServer.receiveMessage(ADDRESS_INBOUND);
    await().atMost(5L, TimeUnit.SECONDS).until(() -> artemisMessage, is(notNullValue()));
  }

  @Test
  void testBlockOnMaxMessages() throws Exception {
    kafkaConsumer.schedulePollTask(() -> {
      kafkaConsumer.rebalance(Collections.singletonList(new TopicPartition(ADDRESS, 0)));
      for (int i = 0; i < 10; ++i) {
        ConsumerRecord kafkaRecord = new ConsumerRecord(ADDRESS, 0, kafkaConsumerOffset.getAndIncrement(), null, String.format("Message: %d", i).getBytes(StandardCharsets.UTF_8));
        kafkaRecord.headers().add("AkbMessageId", String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        kafkaRecord.headers().add("AkbDestinationName", ADDRESS.getBytes(StandardCharsets.UTF_8));
        kafkaRecord.headers().add("AkbRoutingType", "ANYCAST".getBytes(StandardCharsets.UTF_8));
        kafkaConsumer.addRecord(kafkaRecord);
      }
    });

    Map<TopicPartition, Long> startOffsets = new HashMap<>();
    TopicPartition tp = new TopicPartition(ADDRESS, 0);
    startOffsets.put(tp, kafkaConsumerOffset.get());
    kafkaConsumer.updateBeginningOffsets(startOffsets);

    try (
            ServerLocator serverLocator = ActiveMQClient.createServerLocator(artemisServer.getVmURL()); 
            ClientSessionFactory connection = serverLocator.createSessionFactory(); 
            ClientSession session = connection.createSession();
        ) {
      ClientConsumer consumer = session.createConsumer(ADDRESS_INBOUND);
      await().during(2L, TimeUnit.SECONDS).until(() -> artemisServer.getMessageCount(ADDRESS_INBOUND), equalTo(5L));

      consumer.setMessageHandler((artemisMessage) -> {
        try {
          log.debug("Draining message ID: {}", artemisMessage.getMessageID());
          artemisMessage.acknowledge();
          session.commit();
        } catch (Exception e) {
          log.error("Error draining artemis messages.", e);
        }
      });
      session.start();
      await().atMost(10L, TimeUnit.SECONDS).until(() -> artemisServer.getMessageCount(ADDRESS_INBOUND), equalTo(0L));
    }
  }
}
