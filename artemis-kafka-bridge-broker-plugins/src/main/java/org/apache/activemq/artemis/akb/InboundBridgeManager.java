package org.apache.activemq.artemis.akb;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.settings.impl.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.activemq.artemis.akb.kafka.ClientFactory;

public class InboundBridgeManager {

  private static final Logger log = LoggerFactory.getLogger(InboundBridgeManager.class);

  private ActiveMQServer artemisServer;
  private ServerLocator artemisConnectionFactory;
  private ClientFactory kafkaClientFactory;
  private String inboundAddressSuffix;
  private final Set<String> artemisInboundAddressIncludes = new HashSet<>();
  private final Set<String> artemisInboundAddressExcludes = new HashSet<>();

  private Predicate<String> artemisAddressIncludesPredicate;
  private Predicate<String> artemisAddressExcludesPredicate;
  private final Map<String, Set<ServerConsumer>> artemisConsumers = new HashMap<>();
  private final Map<String, InboundBridge> inboundBridges = new HashMap<>();
  private ClientSessionFactory artemisConnection;

  private final ReentrantLock stateLock = new ReentrantLock();

  private boolean running = false;
  private boolean closed = false;

  public ActiveMQServer getArtemisServer() {
    return artemisServer;
  }

  public void setArtemisServer(ActiveMQServer artemisServer) {
    invokeStateChangingOperation(() -> {
      throwIfClosed();
      throwIfRunning();
      this.artemisServer = artemisServer;
    });
  }

  public ServerLocator getArtemisConnectionFactory() {
    return artemisConnectionFactory;
  }

  public void setArtemisConnectionFactory(ServerLocator artemisConnectionFactory) {
    invokeStateChangingOperation(() -> {
      throwIfClosed();
      throwIfRunning();
      this.artemisConnectionFactory = artemisConnectionFactory;
    });
  }

  public ClientFactory getKafkaClientFactory() {
    return kafkaClientFactory;
  }

  public void setKafkaClientFactory(ClientFactory kafkaClientFactory) {
    invokeStateChangingOperation(() -> {
      throwIfClosed();
      throwIfRunning();
      this.kafkaClientFactory = kafkaClientFactory;
    });
  }

  public String getInboundAddressSuffix() {
    return inboundAddressSuffix;
  }

  public void setInboundAddressSuffix(String inboundAddressSuffix) {
    invokeStateChangingOperation(() -> {
      throwIfClosed();
      throwIfRunning();
      this.inboundAddressSuffix = inboundAddressSuffix;
    });
  }

  public Set<String> getArtemisInboundAddressIncludes() {
    return invokeStateChangingOperation(() -> {
      if (closed || running) {
        return Collections.unmodifiableSet(artemisInboundAddressIncludes);
      }
      return artemisInboundAddressIncludes;
    });
  }

  public Set<String> getArtemisInboundAddressExcludes() {
    return invokeStateChangingOperation(() -> {
      if (closed || running) {
        return Collections.unmodifiableSet(artemisInboundAddressExcludes);
      }
      return artemisInboundAddressExcludes;
    });
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isClosed() {
    return closed;
  }

  private void invokeStateChangingOperation(Runnable operation) {
    try {
      stateLock.lock();
      operation.run();
    } finally {
      stateLock.unlock();
    }
  }

  private <T> T invokeStateChangingOperation(Supplier<T> operation) {
    try {
      stateLock.lock();
      return operation.get();
    } finally {
      stateLock.unlock();
    }
  }

  private void throwIfClosed() {
    if (closed) {
      throw new IllegalStateException("This inbound bridge manager is closed.");
    }
  }

  private void throwIfRunning() {
    if (running) {
      throw new IllegalStateException("This inbound bridge manager is currently runnning.");
    }
  }

  private void throwIfNotReady() {
    try {
      Objects.requireNonNull(artemisServer, "The artemisServer has not been set.");
      Objects.requireNonNull(artemisConnectionFactory, "The artemisConnectionFactory has not been set.");
      Objects.requireNonNull(kafkaClientFactory, "The kafkaClientFactory has not been set.");
      Objects.requireNonNull(inboundAddressSuffix, "The inboundAddressSuffix has not been set.");
    } catch (NullPointerException e) {
      throw new IllegalStateException(e.getMessage());
    }
  }

  private void initializeIfNecessary() {
    throwIfNotReady();

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

    if (artemisConnection == null || artemisConnection.isClosed()) {
      try {
        artemisConnection = artemisConnectionFactory.createSessionFactory();
      } catch (Exception e) {
        log.error("Unable to create connection to broker.");
        log.debug("Stack trace:", e);
        throw new RuntimeException(e);
      }
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
    if (shouldInclude) {
      log.debug("Address {} matches includes/excludes predicate. Inbound bridge will be created.", artemisAddress);
      InboundBridge inboundBridge = inboundBridges.get(artemisAddress);
      if (inboundBridge == null) {
        String kafkaAddress = artemisAddress.replaceAll("\\Q" + inboundAddressSuffix + "\\E$", "");
        inboundBridge = new InboundBridge(artemisAddress, artemisConnection, kafkaAddress, kafkaClientFactory);
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
    invokeStateChangingOperation(() -> {
      if (closed) {
        log.debug("Ignoring onConsumerAdded({}). This inbound bridge manager is already closed.", artemisConsumer.getID());
        return;
      }

      String artemisAddress = artemisConsumer.getQueueAddress().toString();
      Set<ServerConsumer> existingArtemisConsumers = artemisConsumers.getOrDefault(artemisAddress, new HashSet<>());
      existingArtemisConsumers.add(artemisConsumer);
      artemisConsumers.put(artemisAddress, existingArtemisConsumers);

      if (running) {
        addNewInboundBridge(artemisAddress, true);
      }
    });
  }

  public void onConsumerRemoved(ServerConsumer artemisConsumer) {
    invokeStateChangingOperation(() -> {
      if (closed) {
        log.debug("Ignoring onConsumerRemoved({}). This inbound bridge manager is already closed.", artemisConsumer.getID());
        return;
      }

      String artemisAddress = artemisConsumer.getQueueAddress().toString();
      Set<ServerConsumer> existingConsumers = artemisConsumers.get(artemisAddress);
      if (existingConsumers != null) {
        existingConsumers.remove(artemisConsumer);
      }
      if (existingConsumers == null || existingConsumers.isEmpty()) {
        InboundBridge inboundBridge = inboundBridges.remove(artemisAddress);
        if (inboundBridge != null) {
          inboundBridge.close();
        }
        artemisConsumers.remove(artemisAddress);
      }
    });
  }

  public synchronized void start() {
    invokeStateChangingOperation(() -> {
      throwIfClosed();
      if (running) {
        return;
      }

      log.debug("Starting {} inbound bridges.", inboundBridges.size());
      initializeIfNecessary();
      for (Map.Entry<String, InboundBridge> inboundBridgesEntry : inboundBridges.entrySet()) {
        try {
          inboundBridgesEntry.getValue().start();
        } catch (Exception e) {
          log.error("Unable to start inbound bridge for {}.", inboundBridgesEntry.getKey());
          log.debug("Stack trace:", e);
        }
      }
      running = true;
    });
  }

  public synchronized void stop() {
    invokeStateChangingOperation(() -> {
      throwIfClosed();
      if (!running) {
        return;
      }

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
    });
  }

  public synchronized void restart() {
    invokeStateChangingOperation(() -> {
      throwIfClosed();
      stop();
      start();
    });
  }

  public synchronized void close() {
    invokeStateChangingOperation(() -> {
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
      closed = true;
    });
  }
}
