package org.apache.activemq.artemis.akb;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class DefaultKafkaClientFactory implements KafkaClientFactory<KafkaProducer, KafkaConsumer> {

  @Override
  public KafkaProducer kafkaProducer() {
    throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
  }

  @Override
  public KafkaConsumer kafkaConsumer() {
    throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
  }
}