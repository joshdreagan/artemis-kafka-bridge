package org.apache.activemq.artemis.akb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.settings.impl.Match;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InboundBridgeManager {

  private static final Logger log = LoggerFactory.getLogger(InboundBridgeManager.class);

  private final Set<String> artemisOutboundAddresses = new HashSet<>();
  private final Set<String> artemisInboundAddresses = new HashSet<>();
  private ActiveMQServer artemisServer;
  private ClientSessionFactory artemisConnection;
  private Consumer<byte[], byte[]> kafkaConsumer;

  private Predicate<String> artemisAddressPredicate;
  private final Map<String, Set<ServerConsumer>> artemisConsumers = new HashMap<>();
  private final Map<String, InboundBridge> inboundBridges = new HashMap<>();

  private boolean running;

  public Set<String> getArtemisOutboundAddresses() {
    return artemisOutboundAddresses;
  }

  public Set<String> getArtemisInboundAddresses() {
    return artemisInboundAddresses;
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

  public Consumer<byte[], byte[]> getKafkaConsumer() {
    return kafkaConsumer;
  }

  public void setKafkaConsumer(Consumer<byte[], byte[]> kafkaConsumer) {
    this.kafkaConsumer = kafkaConsumer;
  }

  public Predicate<String> getArtemisAddressPredicate() {
    return artemisAddressPredicate;
  }

  public void setArtemisAddressPredicate(Predicate<String> artemisAddressPredicate) {
    this.artemisAddressPredicate = artemisAddressPredicate;
  }

  private Predicate<String> artemisAddressPredicate() {
    if (artemisAddressPredicate == null) {
      for (String artemisOutboundAddress : artemisOutboundAddresses) {
        Predicate<String> predicate = Pattern.compile("\\Q" + artemisOutboundAddress + "\\E").asMatchPredicate().negate();
        artemisAddressPredicate = (artemisAddressPredicate == null) ? predicate : artemisAddressPredicate.and(predicate);
      }
      for (String artemisInboundAddress : artemisInboundAddresses) {
        Predicate<String> predicate = Match.createPattern(artemisInboundAddress, artemisServer.getConfiguration().getWildcardConfiguration(), true).asMatchPredicate();
        artemisAddressPredicate = (artemisAddressPredicate == null) ? predicate : artemisAddressPredicate.or(predicate);
      }
    }
    return artemisAddressPredicate;
  }

  public void onConsumerAdded(ServerConsumer artemisConsumer) {
    String artemisAddress = artemisConsumer.getQueueAddress().toString();
    Set<ServerConsumer> existingArtemisConsumers = artemisConsumers.getOrDefault(artemisAddress, new HashSet<>());
    existingArtemisConsumers.add(artemisConsumer);
    artemisConsumers.put(artemisAddress, existingArtemisConsumers);

    if (running) {
      boolean shouldInclude = artemisAddressPredicate().test(artemisAddress);
      log.debug("Address {} {} predicate.", artemisAddress, (shouldInclude) ? "matches" : "does not match");
      if (shouldInclude) {
        InboundBridge inboundBridge = inboundBridges.get(artemisAddress);
        if (inboundBridge == null) {
          inboundBridge = new InboundBridge(artemisAddress, this);
          inboundBridge.start();
          inboundBridges.put(artemisAddress, inboundBridge);
        }
      }
    }
  }

  public void onConsumerRemoved(ServerConsumer artemisConsumer) {
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
    if (!running) {
      for (Map.Entry<String, Set<ServerConsumer>> artemisConsumersEntry : artemisConsumers.entrySet()) {
        String artemisAddress = artemisConsumersEntry.getKey();
        Set<ServerConsumer> existingConsumers = artemisConsumersEntry.getValue();
        if (!existingConsumers.isEmpty() && !inboundBridges.containsKey(artemisAddress)) {
          boolean shouldInclude = artemisAddressPredicate().test(artemisAddress);
          log.debug("Address {} {} predicate.", artemisAddress, (shouldInclude) ? "matches" : "does not match");
          if (shouldInclude) {
            InboundBridge inboundBridge = inboundBridges.get(artemisAddress);
            if (inboundBridge == null) {
              inboundBridge = new InboundBridge(artemisAddress, this);
              inboundBridges.put(artemisAddress, inboundBridge);
            }
          }
        }
      }
      log.debug("Starting {} inbound bridges.", inboundBridges.size());
      for (Map.Entry<String, InboundBridge> inboundBridgesEntry : inboundBridges.entrySet()) {
        inboundBridgesEntry.getValue().start();
      }
      running = true;
    }
  }

  public void stop() {
    if (running) {
      log.debug("Stopping {} inbound bridges.", inboundBridges.size());
      Set<String> inboundBridgesToRemove = new HashSet<>();
      for (Map.Entry<String, InboundBridge> inboundBridgesEntry : inboundBridges.entrySet()) {
        inboundBridgesEntry.getValue().stop();
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
    stop();
    start();
  }
}
