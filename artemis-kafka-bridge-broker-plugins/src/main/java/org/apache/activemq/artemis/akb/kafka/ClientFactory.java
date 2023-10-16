package org.apache.activemq.artemis.akb.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public interface ClientFactory<P extends Producer, C extends Consumer> {
  
  P createKafkaProducer();
  
  C createKafkaConsumer();
}
