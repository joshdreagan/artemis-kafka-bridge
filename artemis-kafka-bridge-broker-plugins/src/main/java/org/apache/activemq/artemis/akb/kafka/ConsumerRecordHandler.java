package org.apache.activemq.artemis.akb.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerRecordHandler<K,V> {
  
  public void onConsumerRecord(ConsumerRecord<K,V> consumerRecord);
}
