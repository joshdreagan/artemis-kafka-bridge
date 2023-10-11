package org.apache.activemq.artemis.akb;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.junit.EmbeddedActiveMQExtension;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.awaitility.Awaitility.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unused")
public class ArtemisKafkaBrokerPluginTest {

  @RegisterExtension
  private static EmbeddedActiveMQExtension artemisServer = new EmbeddedActiveMQExtension("broker.xml");

  private static MockKafkaClientFactory kafkaClientFactory = new MockKafkaClientFactory();

  @AfterEach
  void afterEach() {
    kafkaClientFactory.kafkaProducer().clear();
  }

  @Test
  void testArtemisProducer() throws Exception {
    artemisServer.sendMessage("app.foo", "Holy crap it works!!!");

    await().atMost(600L, TimeUnit.SECONDS).until(() -> kafkaClientFactory.kafkaProducer().history().size(), equalTo(1));
  }

  @Test
  void testArtemisConsumer() throws Exception {
    /*&
    ConsumerRecord kafkaMessage = new ConsumerRecord("app.foo", 0, 0, null, "Holy crap it works!!!".getBytes(StandardCharsets.UTF_8));
    //ProducerRecord kafkaMessage = new ProducerRecord("app.foo", "Holy crap it works!!!".getBytes(StandardCharsets.UTF_8));
    kafkaMessage.headers().add("AkbMessageId", "12345".getBytes(StandardCharsets.UTF_8));
    kafkaMessage.headers().add("AkbDestinationName", "app.foo".getBytes(StandardCharsets.UTF_8));
    kafkaMessage.headers().add("AkbRoutingType", "ANYCAST".getBytes(StandardCharsets.UTF_8));
    kafkaClientFactory.kafkaConsumer().addRecord(kafkaMessage);
    //kafkaClientFactory.kafkaProducer().send(kafkaMessage);
    */

    ClientMessage artemisMessage = artemisServer.receiveMessage("app.foo");
    assertNotNull(artemisMessage);
  }
}
