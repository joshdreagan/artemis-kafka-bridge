package org.apache.activemq.artemis.akb;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQExtension;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.awaitility.Awaitility.*;
import static org.hamcrest.Matchers.*;

@SuppressWarnings("unused")
public class ArtemisKafkaBrokerPluginTest {

  @RegisterExtension
  private static EmbeddedActiveMQExtension artemisServer = new EmbeddedActiveMQExtension("broker.xml");

  private static MockKafkaClientFactory kafkaClientFactory = new MockKafkaClientFactory();

  private MockProducer<byte[], byte[]> kafkaProducer;
  private MockConsumer<byte[], byte[]> kafkaConsumer;

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
  @Disabled
  void testArtemisProducer() throws Exception {
    artemisServer.sendMessage("app.foo", "Holy crap it works!!!");

    await().atMost(5L, TimeUnit.SECONDS).until(() -> kafkaProducer.history().size(), equalTo(1));
  }

  @Test
  void testArtemisConsumer() throws Exception {
    kafkaConsumer.schedulePollTask(() -> {
      kafkaConsumer.rebalance(Collections.singletonList(new TopicPartition("app.foo", 0)));
      ConsumerRecord kafkaRecord = new ConsumerRecord("app.foo", 0, 0, null, "Holy crap it works!!!".getBytes(StandardCharsets.UTF_8));
      kafkaRecord.headers().add("AkbMessageId", "12345".getBytes(StandardCharsets.UTF_8));
      kafkaRecord.headers().add("AkbDestinationName", "app.foo".getBytes(StandardCharsets.UTF_8));
      kafkaRecord.headers().add("AkbRoutingType", "ANYCAST".getBytes(StandardCharsets.UTF_8));
      kafkaConsumer.addRecord(kafkaRecord);
    });

    Map<TopicPartition, Long> startOffsets = new HashMap<>();
    TopicPartition tp = new TopicPartition("app.foo", 0);
    startOffsets.put(tp, 0L);
    kafkaConsumer.updateBeginningOffsets(startOffsets);

    artemisServer.setDefaultReceiveTimeout(30000L);
    ClientMessage artemisMessage = artemisServer.receiveMessage("app.foo.inbound");
    await().atMost(15L, TimeUnit.SECONDS).until(() -> artemisMessage, is(notNullValue()));
  }
}
