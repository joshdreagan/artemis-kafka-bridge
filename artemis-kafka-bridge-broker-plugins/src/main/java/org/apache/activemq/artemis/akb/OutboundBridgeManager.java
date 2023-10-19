package org.apache.activemq.artemis.akb;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import javax.jms.Connection;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.activemq.artemis.akb.kafka.ClientFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class OutboundBridgeManager {

  private static final Logger log = LoggerFactory.getLogger(OutboundBridgeManager.class);

  private ActiveMQServer artemisServer;
  private ActiveMQConnectionFactory artemisConnectionFactory;
  private ClientFactory kafkaClientFactory;
  private final Set<String> artemisOutboundAddresses = new HashSet<>();

  private final Map<String, OutboundBridge> outboundBridges = new HashMap<>();
  private Connection artemisConnection;

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

  public ActiveMQConnectionFactory getArtemisConnectionFactory() {
    return artemisConnectionFactory;
  }

  public void setArtemisConnectionFactory(ActiveMQConnectionFactory artemisConnectionFactory) {
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

  public Set<String> getArtemisOutboundAddresses() {
    return invokeStateChangingOperation(() -> {
      if (closed || running) {
        return Collections.unmodifiableSet(artemisOutboundAddresses);
      }
      return artemisOutboundAddresses;
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
      throw new IllegalStateException("This outbound bridge manager is closed.");
    }
  }

  private void throwIfRunning() {
    if (running) {
      throw new IllegalStateException("This outbound bridge manager is currently running.");
    }
  }

  private void throwIfNotReady() {
    try {
      Objects.requireNonNull(artemisServer, "The artemisServer has not been set.");
      Objects.requireNonNull(artemisConnectionFactory, "The artemisConnectionFactory has not been set.");
      Objects.requireNonNull(kafkaClientFactory, "The kafkaClientFactory has not been set.");
    } catch (NullPointerException e) {
      throw new IllegalStateException(e.getMessage());
    }
  }

  private void initializeIfNecessary() {
    throwIfNotReady();

    if (artemisConnection == null) {
      try {
        artemisConnection = artemisConnectionFactory.createConnection();
        artemisConnection.start();
      } catch (Exception e) {
        log.error("Unable to create connection to broker.");
        log.debug("Stack trace:", e);
        throw new RuntimeException(e);
      }
    }
    for (String artemisOutboundAddress : artemisOutboundAddresses) {
      OutboundBridge outboundBridge = outboundBridges.get(artemisOutboundAddress);
      if (outboundBridge == null) {
        outboundBridge = new OutboundBridge(artemisOutboundAddress, artemisConnection, kafkaClientFactory);
        outboundBridges.put(artemisOutboundAddress, outboundBridge);
      }
    }
  }

  public synchronized void start() {
    invokeStateChangingOperation(() -> {
      throwIfClosed();
      if (running) {
        return;
      }

      log.debug("Starting {} outbound bridges.", outboundBridges.size());
      initializeIfNecessary();
      for (Map.Entry<String, OutboundBridge> outboundBridgesEntry : outboundBridges.entrySet()) {
        try {
          outboundBridgesEntry.getValue().start();
        } catch (Exception e) {
          log.error("Unable to start outbound bridge for {}.", outboundBridgesEntry.getKey());
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
          artemisConnection.stop();
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
