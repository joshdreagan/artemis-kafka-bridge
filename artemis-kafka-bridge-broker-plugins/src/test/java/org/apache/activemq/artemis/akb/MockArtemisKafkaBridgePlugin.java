package org.apache.activemq.artemis.akb;

import java.util.Map;

public class MockArtemisKafkaBridgePlugin extends ArtemisKafkaBridgePlugin {

  @Override
  public void initKafkaClientFactory(Map<String, String> properties) {
    kafkaClientFactory = new MockKafkaClientFactory();
  }
}
