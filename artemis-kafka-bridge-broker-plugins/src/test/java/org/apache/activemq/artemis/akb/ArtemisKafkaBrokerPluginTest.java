package org.apache.activemq.artemis.akb;

import org.apache.activemq.artemis.akb.kafka.MockClientFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQExtension;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.awaitility.Awaitility.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

@SuppressWarnings("unused")
public class ArtemisKafkaBrokerPluginTest {

  private static final Logger log = LoggerFactory.getLogger(ArtemisKafkaBrokerPluginTest.class);

  public static final String ADDRESS = "app.foo";
  public static final String ADDRESS_INBOUND = "app.foo.inbound";

  @RegisterExtension
  private static EmbeddedActiveMQExtension artemisServer = new EmbeddedActiveMQExtension("broker.xml");
  private static ConnectionFactory artemisConnectionFactory;

  private static MockClientFactory kafkaClientFactory = new MockClientFactory();
  private static AtomicLong kafkaConsumerOffset = new AtomicLong(0);

  private Connection artemisConnection;
  private Session artemisSession;

  private MockProducer<byte[], byte[]> kafkaProducer;
  private MockConsumer<byte[], byte[]> kafkaConsumer;

  @BeforeAll
  static void beforeAll() throws Exception {
    artemisConnectionFactory = ActiveMQJMSClient.createConnectionFactory(artemisServer.getVmURL(), null);
  }

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
    artemisConnection = artemisConnectionFactory.createConnection();
    artemisSession = artemisConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

    kafkaProducer = kafkaClientFactory.createKafkaProducer();
    kafkaConsumer = kafkaClientFactory.createKafkaConsumer();
  }

  @AfterEach
  void afterEach() throws Exception {
    kafkaProducer.clear();

    artemisSession.close();
    artemisConnection.close();
  }

  @Test
  @DisplayName("testArtemisProducer")
  void testArtemisProducer() throws Exception {
    MessageProducer artemisProducer = artemisSession.createProducer(artemisSession.createQueue(ADDRESS));
    TextMessage message = artemisSession.createTextMessage();
    message.setText("Holy crap it works!!!");
    artemisProducer.send(message);
    await().atMost(60L, TimeUnit.SECONDS).until(() -> kafkaProducer.history().size(), equalTo(1));
  }

  @Test
  void testArtemisConsumer() throws Exception {
    kafkaConsumer.schedulePollTask(() -> {
      kafkaConsumer.rebalance(Collections.singletonList(new TopicPartition(ADDRESS, 0)));
      ConsumerRecord kafkaRecord = new ConsumerRecord(ADDRESS, 0, kafkaConsumerOffset.getAndIncrement(), null, "Holy crap it works!!!".getBytes(StandardCharsets.UTF_8));
      kafkaRecord.headers().add(AkbHeaders.HDR_AKB_MESSAGE_ID, "12345".getBytes(StandardCharsets.UTF_8));
      kafkaRecord.headers().add(AkbHeaders.HDR_AKB_MESSAGE_TYPE, AkbMessageType.TEXT.name().getBytes(StandardCharsets.UTF_8));
      kafkaRecord.headers().add(AkbHeaders.HDR_AKB_DESTINATION_NAME, ADDRESS.getBytes(StandardCharsets.UTF_8));
      kafkaRecord.headers().add(AkbHeaders.HDR_AKB_ROUTING_TYPE, RoutingType.ANYCAST.name().getBytes(StandardCharsets.UTF_8));
      kafkaConsumer.addRecord(kafkaRecord);
    });

    Map<TopicPartition, Long> startOffsets = new HashMap<>();
    TopicPartition tp = new TopicPartition(ADDRESS, 0);
    startOffsets.put(tp, kafkaConsumerOffset.get());
    kafkaConsumer.updateBeginningOffsets(startOffsets);

    MessageConsumer artemisConsumer = artemisSession.createConsumer(artemisSession.createQueue(ADDRESS_INBOUND));
    artemisConnection.start();
    TextMessage artemisMessage = (TextMessage) artemisConsumer.receive(30000L);
    await().atMost(5L, TimeUnit.SECONDS).until(() -> artemisMessage, is(notNullValue()));
    artemisMessage.acknowledge();
    assertThat(artemisMessage.getText(), equalTo("Holy crap it works!!!"));
  }

  @Test
  void testBlockOnMaxMessages() throws Exception {
    kafkaConsumer.schedulePollTask(() -> {
      kafkaConsumer.rebalance(Collections.singletonList(new TopicPartition(ADDRESS, 0)));
      for (int i = 0; i < 10; ++i) {
        ConsumerRecord kafkaRecord = new ConsumerRecord(ADDRESS, 0, kafkaConsumerOffset.getAndIncrement(), null, String.format("Message: %d", i).getBytes(StandardCharsets.UTF_8));
        kafkaRecord.headers().add(AkbHeaders.HDR_AKB_MESSAGE_ID, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        kafkaRecord.headers().add(AkbHeaders.HDR_AKB_MESSAGE_TYPE, AkbMessageType.TEXT.name().getBytes(StandardCharsets.UTF_8));
        kafkaRecord.headers().add(AkbHeaders.HDR_AKB_DESTINATION_NAME, ADDRESS.getBytes(StandardCharsets.UTF_8));
        kafkaRecord.headers().add(AkbHeaders.HDR_AKB_ROUTING_TYPE, RoutingType.ANYCAST.name().getBytes(StandardCharsets.UTF_8));
        kafkaConsumer.addRecord(kafkaRecord);
      }
    });

    Map<TopicPartition, Long> startOffsets = new HashMap<>();
    TopicPartition tp = new TopicPartition(ADDRESS, 0);
    startOffsets.put(tp, kafkaConsumerOffset.get());
    kafkaConsumer.updateBeginningOffsets(startOffsets);

    MessageConsumer artemisConsumer = artemisSession.createConsumer(artemisSession.createQueue(ADDRESS_INBOUND));
    artemisConnection.start();
    await().during(2L, TimeUnit.SECONDS).until(() -> artemisServer.getMessageCount(ADDRESS_INBOUND), equalTo(5L));

    artemisConsumer.setMessageListener((artemisMessage) -> {
      try {
        log.debug("Draining message ID: {}", artemisMessage.getJMSMessageID());
        artemisMessage.acknowledge();
      } catch (Exception e) {
        log.error("Error draining artemis messages.", e);
      }
    });
    await().atMost(10L, TimeUnit.SECONDS).until(() -> artemisServer.getMessageCount(ADDRESS_INBOUND), equalTo(0L));
  }
}
