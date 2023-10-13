package org.apache.activemq.artemis.akb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundBridgeManager {

  private static final Logger log = LoggerFactory.getLogger(OutboundBridgeManager.class);

  private ActiveMQServer artemisServer;
  private ServerLocator artemisConnectionFactory;
  private KafkaClientFactory kafkaClientFactory;
  private final Set<String> artemisOutboundAddresses = new HashSet<>();

  private final Map<String, OutboundBridge> outboundBridges = new HashMap<>();
  private ClientSessionFactory artemisConnection;

  private boolean running = false;
  private boolean closed = false;

  public ActiveMQServer getArtemisServer() {
    checkClosed();
    return artemisServer;
  }

  public void setArtemisServer(ActiveMQServer artemisServer) {
    checkClosed();
    this.artemisServer = artemisServer;
  }

  public ServerLocator getArtemisConnectionFactory() {
    checkClosed();
    return artemisConnectionFactory;
  }

  public void setArtemisConnectionFactory(ServerLocator artemisConnectionFactory) {
    checkClosed();
    this.artemisConnectionFactory = artemisConnectionFactory;
  }

  public KafkaClientFactory getKafkaClientFactory() {
    checkClosed();
    return kafkaClientFactory;
  }

  public void setKafkaClientFactory(KafkaClientFactory kafkaClientFactory) {
    checkClosed();
    this.kafkaClientFactory = kafkaClientFactory;
  }

  public Set<String> getArtemisOutboundAddresses() {
    checkClosed();
    return artemisOutboundAddresses;
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isClosed() {
    return closed;
  }
  
  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("This outbound bridge manager is closed.");
    }
  }

  private void checkState() {
    try {
      Objects.requireNonNull(artemisServer, "The artemisServer has not been set.");
      Objects.requireNonNull(artemisConnectionFactory, "The artemisConnectionFactory has not been set.");
      Objects.requireNonNull(kafkaClientFactory, "The kafkaClientFactory has not been set.");
    } catch (NullPointerException e) {
      throw new IllegalStateException(e.getMessage());
    }
  }

  private void initialize() {
    try {
      artemisConnection = artemisConnectionFactory.createSessionFactory();
    } catch (Exception e) {
      log.error("Unable to create connection to broker.");
      log.debug("Stack trace:", e);
      throw new RuntimeException(e);
    }

    for (String artemisOutboundAddress : artemisOutboundAddresses) {
      OutboundBridge outboundBridge = outboundBridges.get(artemisOutboundAddress);
      if (outboundBridge == null) {
        outboundBridge = new OutboundBridge(artemisOutboundAddress, artemisConnection, kafkaClientFactory.createKafkaProducer());
        outboundBridges.put(artemisOutboundAddress, outboundBridge);
      }
    }
  }

  public void start() {
    checkClosed();
    if (!running) {
      checkState();
      initialize();
      log.debug("Starting {} outbound bridges.", outboundBridges.size());
      for (Map.Entry<String, OutboundBridge> outboundBridgesEntry : outboundBridges.entrySet()) {
        try {
          outboundBridgesEntry.getValue().start();
        } catch (Exception e) {
          log.error("Unable to start outbound bridge for {}.", outboundBridgesEntry.getKey());
          log.debug("Stack trace:", e);
        }
      }
      running = true;
    }
  }

  public void stop() {
    checkClosed();
    if (running) {
      log.debug("Stopping {} outbound bridges.", outboundBridges.size());
      Set<String> outboundBridgesToRemove = new HashSet<>();
      for (Map.Entry<String, OutboundBridge> outboundBridgesEntry : outboundBridges.entrySet()) {
        try {
          outboundBridgesEntry.getValue().stop();
        } catch (Exception e) {
          log.error("Unable to stop outbound bridge for {}.", outboundBridgesEntry.getKey());
          log.debug("Stack trace:", e);
        }
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
    checkClosed();
    stop();
    start();
  }

  public void close() {
    stop();
    log.debug("Closing {} outbound bridges.", outboundBridges.size());
    try {
      for (Map.Entry<String, OutboundBridge> outboundBridgesEntry : outboundBridges.entrySet()) {
        try {
          outboundBridgesEntry.getValue().close();
        } catch (Exception e) {
          log.error("Unable to close outbound bridge for {}.", outboundBridgesEntry.getKey());
          log.debug("Stack trace:", e);
        }
      }
    } finally {
      outboundBridges.clear();
    }
    if (artemisConnection != null) {
      try {
        artemisConnection.close();
      } catch (Exception e) {
        log.error("Unable to close artemis connection.");
        log.debug("Stack trace:", e);
      } finally {
        artemisConnection = null;
      }
    }
    artemisConnectionFactory = null;
    kafkaClientFactory = null;
    artemisOutboundAddresses.clear();
    artemisServer = null;
    closed = true;
  }
}
