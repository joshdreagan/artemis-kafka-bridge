package org.apache.activemq.artemis.akb;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public interface KafkaClientFactory<P extends Producer, C extends Consumer> {
  
  P kafkaProducer();
  
  C kafkaConsumer();
}
