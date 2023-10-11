package org.apache.activemq.artemis.akb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundBridgeManager {

  private static final Logger log = LoggerFactory.getLogger(OutboundBridgeManager.class);

  private final Set<String> artemisOutboundAddresses = new HashSet<>();
  private ActiveMQServer artemisServer;
  private ClientSessionFactory artemisConnection;
  private Producer<byte[], byte[]> kafkaProducer;

  private final Map<String, OutboundBridge> outboundBridges = new HashMap<>();
  private boolean running = false;

  public Set<String> getArtemisOutboundAddresses() {
    return artemisOutboundAddresses;
  }

  public ActiveMQServer getArtemisServer() {
    return artemisServer;
  }

  public void setArtemisServer(ActiveMQServer artemisServer) {
    this.artemisServer = artemisServer;
  }

  public ClientSessionFactory getArtemisConnection() {
    return artemisConnection;
  }

  public void setArtemisConnection(ClientSessionFactory artemisConnection) {
    this.artemisConnection = artemisConnection;
  }

  public Producer<byte[], byte[]> getKafkaProducer() {
    return kafkaProducer;
  }

  public void setKafkaProducer(Producer<byte[], byte[]> kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
  }

  public void start() {
    if (!running) {
      for (String artemisOutboundAddress : artemisOutboundAddresses) {
        OutboundBridge outboundBridge = outboundBridges.get(artemisOutboundAddress);
        if (outboundBridge == null) {
          outboundBridge = new OutboundBridge(artemisOutboundAddress, this);
          outboundBridges.put(artemisOutboundAddress, outboundBridge);
        }
      }
      log.debug("Starting {} outbound bridges.", outboundBridges.size());
      for (Map.Entry<String, OutboundBridge> outboundBridgesEntry : outboundBridges.entrySet()) {
        outboundBridgesEntry.getValue().start();
      }
      running = true;
    }
  }

  public void stop() {
    if (running) {
      log.debug("Stopping {} outbound bridges.", outboundBridges.size());
      Set<String> outboundBridgesToRemove = new HashSet<>();
      for (Map.Entry<String, OutboundBridge> outboundBridgesEntry : outboundBridges.entrySet()) {
        outboundBridgesEntry.getValue().stop();
        if (!artemisOutboundAddresses.contains(outboundBridgesEntry.getKey())) {
          outboundBridgesToRemove.add(outboundBridgesEntry.getKey());
        }
      }
      for (String outboundBridgeAddress : outboundBridgesToRemove) {
        outboundBridges.remove(outboundBridgeAddress);
      }
      running = false;
    }
  }

  public void restart() {
    stop();
    start();
  }
}
