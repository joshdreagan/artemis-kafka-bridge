package org.apache.activemq.artemis.akb.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class DefaultClientFactory implements ClientFactory<KafkaProducer, KafkaConsumer> {

  private final Map<String, String> commonProperties = new HashMap<>();
  private final Map<String, String> producerProperties = new HashMap<>();
  private final Map<String, String> consumerProperties = new HashMap<>();
  
  public DefaultClientFactory() {
  }

  public Map<String, String> getCommonProperties() {
    return commonProperties;
  }

  public Map<String, String> getProducerProperties() {
    return producerProperties;
  }

  public Map<String, String> getConsumerProperties() {
    return consumerProperties;
  }

  @Override
  public KafkaProducer createKafkaProducer() {
    Map<String, String> combinedProperties = new HashMap<>();
    combinedProperties.putAll(commonProperties);
    combinedProperties.putAll(producerProperties);
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    KafkaProducer producer = new KafkaProducer(combinedProperties);
    Thread.currentThread().setContextClassLoader(originalClassLoader);
    return producer;
  }

  @Override
  public KafkaConsumer createKafkaConsumer() {
    Map<String, String> combinedProperties = new HashMap<>();
    combinedProperties.putAll(commonProperties);
    combinedProperties.putAll(consumerProperties);
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    KafkaConsumer consumer = new KafkaConsumer(combinedProperties);
    Thread.currentThread().setContextClassLoader(originalClassLoader);
    return consumer;
  }
}
