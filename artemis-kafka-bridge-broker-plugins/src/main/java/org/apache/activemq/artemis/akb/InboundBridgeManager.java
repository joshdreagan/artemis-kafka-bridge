package org.apache.activemq.artemis.akb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.settings.impl.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InboundBridgeManager {

  private static final Logger log = LoggerFactory.getLogger(InboundBridgeManager.class);

  private ActiveMQServer artemisServer;
  private ServerLocator artemisConnectionFactory;
  private KafkaClientFactory kafkaClientFactory;
  private final Set<String> artemisInboundAddressIncludes = new HashSet<>();
  private final Set<String> artemisInboundAddressExcludes = new HashSet<>();

  private Predicate<String> artemisAddressIncludesPredicate;
  private Predicate<String> artemisAddressExcludesPredicate;
  private final Map<String, Set<ServerConsumer>> artemisConsumers = new HashMap<>();
  private final Map<String, InboundBridge> inboundBridges = new HashMap<>();
  private ClientSessionFactory artemisConnection;

  private boolean running;
  private boolean closed;

  public ActiveMQServer getArtemisServer() {
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

  public Set<String> getArtemisInboundAddressIncludes() {
    checkClosed();
    return artemisInboundAddressIncludes;
  }

  public Set<String> getArtemisInboundAddressExcludes() {
    checkClosed();
    return artemisInboundAddressExcludes;
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isClosed() {
    return closed;
  }

  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("This inbound bridge manager is closed.");
    }
  }
  
  private void checkState() {
    checkClosed();
    try {
      Objects.requireNonNull(artemisServer, "The artemisServer has not been set.");
      Objects.requireNonNull(artemisConnectionFactory, "The artemisConnectionFactory has not been set.");
      Objects.requireNonNull(kafkaClientFactory, "The kafkaClientFactory has not been set.");
    } catch (NullPointerException e) {
      throw new IllegalStateException(e.getMessage());
    }
  }

  private void initialize() {
    artemisAddressIncludesPredicate = null;
    for (String artemisAddress : artemisInboundAddressIncludes) {
      Predicate<String> predicate = Match.createPattern(artemisAddress, artemisServer.getConfiguration().getWildcardConfiguration(), true).asMatchPredicate();
      artemisAddressIncludesPredicate = (artemisAddressIncludesPredicate == null) ? predicate : artemisAddressIncludesPredicate.or(predicate);
    }
    artemisAddressExcludesPredicate = null;
    for (String artemisAddress : artemisInboundAddressExcludes) {
      Predicate<String> predicate = Match.createPattern(artemisAddress, artemisServer.getConfiguration().getWildcardConfiguration(), true).asMatchPredicate();
      artemisAddressExcludesPredicate = (artemisAddressExcludesPredicate == null) ? predicate : artemisAddressExcludesPredicate.or(predicate);
    }

    try {
      artemisConnection = artemisConnectionFactory.createSessionFactory();
    } catch (Exception e) {
      log.error("Unable to create connection to broker.");
      log.debug("Stack trace:", e);
      throw new RuntimeException(e);
    }

    for (Map.Entry<String, Set<ServerConsumer>> artemisConsumersEntry : artemisConsumers.entrySet()) {
      String artemisAddress = artemisConsumersEntry.getKey();
      Set<ServerConsumer> existingConsumers = artemisConsumersEntry.getValue();
      if (!existingConsumers.isEmpty() && !inboundBridges.containsKey(artemisAddress)) {
        addNewInboundBridge(artemisAddress, false);
      }
    }
  }

  private boolean shouldInclude(String artemisAddress) {
    if (artemisAddressIncludesPredicate == null) {
      return false;
    }
    if (artemisAddressExcludesPredicate != null && artemisAddressExcludesPredicate.test(artemisAddress)) {
      return false;
    }
    return artemisAddressIncludesPredicate.test(artemisAddress);
  }

  private boolean addNewInboundBridge(String artemisAddress, boolean start) {
    boolean added = false;
    boolean shouldInclude = shouldInclude(artemisAddress);
    log.debug("Address {} {} predicate.", artemisAddress, (shouldInclude) ? "matches" : "does not match");
    if (shouldInclude) {
      InboundBridge inboundBridge = inboundBridges.get(artemisAddress);
      if (inboundBridge == null) {
        inboundBridge = new InboundBridge(artemisAddress, artemisConnection, kafkaClientFactory.createKafkaConsumer());
        if (start) {
          inboundBridge.start();
        }
        inboundBridges.put(artemisAddress, inboundBridge);
        added = true;
      }
    }
    return added;
  }

  public void onConsumerAdded(ServerConsumer artemisConsumer) {
    checkClosed();
    String artemisAddress = artemisConsumer.getQueueAddress().toString();
    Set<ServerConsumer> existingArtemisConsumers = artemisConsumers.getOrDefault(artemisAddress, new HashSet<>());
    existingArtemisConsumers.add(artemisConsumer);
    artemisConsumers.put(artemisAddress, existingArtemisConsumers);

    if (running) {
      addNewInboundBridge(artemisAddress, true);
    }
  }

  public void onConsumerRemoved(ServerConsumer artemisConsumer) {
    checkClosed();
    String artemisAddress = artemisConsumer.getQueueAddress().toString();
    Set<ServerConsumer> existingConsumers = artemisConsumers.get(artemisAddress);
    if (existingConsumers == null || existingConsumers.isEmpty()) {
      InboundBridge inboundBridge = inboundBridges.remove(artemisAddress);
      if (inboundBridge != null) {
        inboundBridge.stop();
      }
      artemisConsumers.remove(artemisAddress);
    }
  }

  public void start() {
    checkClosed();
    if (!running) {
      checkState();
      initialize();
      log.debug("Starting {} inbound bridges.", inboundBridges.size());
      for (Map.Entry<String, InboundBridge> inboundBridgesEntry : inboundBridges.entrySet()) {
        try {
          inboundBridgesEntry.getValue().start();
        } catch (Exception e) {
          log.error("Unable to start inbound bridge for {}.", inboundBridgesEntry.getKey());
          log.debug("Stack trace:", e);
        }
      }
      running = true;
    }
  }

  public void stop() {
    checkClosed();
    if (running) {
      log.debug("Stopping {} inbound bridges.", inboundBridges.size());
      Set<String> inboundBridgesToRemove = new HashSet<>();
      for (Map.Entry<String, InboundBridge> inboundBridgesEntry : inboundBridges.entrySet()) {
        try {
          inboundBridgesEntry.getValue().stop();
        } catch (Exception e) {
          log.error("Unable to stop inbound bridge for {}.", inboundBridgesEntry.getKey());
          log.debug("Stack trace:", e);
        }
        String artemisAddress = inboundBridgesEntry.getKey();
        Set<ServerConsumer> existingConsumers = artemisConsumers.get(artemisAddress);
        if (existingConsumers == null || existingConsumers.isEmpty()) {
          inboundBridgesToRemove.add(artemisAddress);
        }
      }
      for (String inboundBridgeAddress : inboundBridgesToRemove) {
        inboundBridges.remove(inboundBridgeAddress);
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
    log.debug("Closing {} inbound bridges.", inboundBridges.size());
    try {
      for (Map.Entry<String, InboundBridge> inboundBridgesEntry : inboundBridges.entrySet()) {
        try {
          inboundBridgesEntry.getValue().close();
        } catch (Exception e) {
          log.error("Unable to close inbound bridge for {}.", inboundBridgesEntry.getKey());
          log.debug("Stack trace:", e);
        }
      }
    } finally {
      inboundBridges.clear();
    }
    artemisConsumers.clear();
    artemisAddressIncludesPredicate = null;
    artemisAddressExcludesPredicate = null;
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
    artemisInboundAddressExcludes.clear();
    artemisInboundAddressIncludes.clear();
    artemisServer = null;
    closed = true;
  }
}
