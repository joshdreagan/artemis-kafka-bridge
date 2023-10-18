package org.apache.activemq.artemis.akb.kafka;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class MockClientFactory implements ClientFactory<MockProducer, MockConsumer> {

  private static MockProducer kafkaProducer;
  private static MockConsumer kafkaConsumer;

  @Override
  public MockProducer createKafkaProducer() {
    if (kafkaProducer == null) {
      kafkaProducer = new MockProducer(false, new ByteArraySerializer(), new ByteArraySerializer());
    }
    return kafkaProducer;
  }

  @Override
  public MockConsumer createKafkaConsumer() {
    if (kafkaConsumer == null) {
      kafkaConsumer = new MockConsumer(OffsetResetStrategy.EARLIEST);
    }
    return kafkaConsumer;
  }
}
