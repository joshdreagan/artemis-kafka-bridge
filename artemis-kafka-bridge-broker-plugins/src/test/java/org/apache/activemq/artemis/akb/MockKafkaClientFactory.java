package org.apache.activemq.artemis.akb;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class MockKafkaClientFactory implements KafkaClientFactory<MockProducer, MockConsumer> {

  private static MockProducer kafkaProducer;
  private static MockConsumer kafkaConsumer;
  
  @Override
  public MockProducer kafkaProducer() {
    if (kafkaProducer == null) {
      kafkaProducer = new MockProducer(false, new ByteArraySerializer(), new ByteArraySerializer());
    }
    return kafkaProducer;
  }

  @Override
  public MockConsumer kafkaConsumer() {
    if (kafkaConsumer == null) {
      kafkaConsumer = new MockConsumer(OffsetResetStrategy.EARLIEST);
    }
    return kafkaConsumer;
  }
}
