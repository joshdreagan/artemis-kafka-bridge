package org.apache.activemq.artemis.akb;

import org.apache.activemq.artemis.akb.kafka.MockClientFactory;
import java.util.Map;

public class MockArtemisKafkaBridgePlugin extends ArtemisKafkaBridgePlugin {

  @Override
  public void initKafkaClientFactory(Map<String, String> properties) {
    kafkaClientFactory = new MockClientFactory();
  }
}
