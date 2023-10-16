package org.apache.activemq.artemis.akb.kafka;

import java.nio.file.Path;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class DefaultClientFactory implements ClientFactory<KafkaProducer, KafkaConsumer> {

  public DefaultClientFactory() {
  }

  @Override
  public KafkaProducer createKafkaProducer() {
    throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
  }

  @Override
  public KafkaConsumer createKafkaConsumer() {
    throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
  }
}
