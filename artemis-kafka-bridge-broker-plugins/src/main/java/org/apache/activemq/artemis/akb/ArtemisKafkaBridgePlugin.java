package org.apache.activemq.artemis.akb;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.awaitility.Awaitility.*;
import static org.hamcrest.Matchers.*;

public class ArtemisKafkaBridgePlugin implements ActiveMQServerPlugin {

  private static final Logger log = LoggerFactory.getLogger(ArtemisKafkaBridgePlugin.class);

  public static final String OUTBOUND_ADDRESSES = "akb.outbound-addresses";
  public static final String INBOUND_ADDRESSES = "akb.inbound-addresses";

  public static final String DEFAULT_OUTBOUND_ADDRESSES = "__akb.outbound";
  public static final String DEFAULT_ADDRESS_SPLIT_REGEX = "\\s*[,:;\\s]\\s*";

  protected ServerLocator artemisServerLocator;
  protected KafkaClientFactory kafkaClientFactory;
  protected OutboundBridgeManager outboundBridgeManager;
  protected InboundBridgeManager inboundBridgeManager;

  protected ActiveMQServer server;
  protected ClientSessionFactory artemisConnection;

  @Override
  public void init(Map<String, String> properties) {
    log.debug("{} plugin init.", this.getClass().getSimpleName());
    initArtemisServerLocator(properties);
    initKafkaClientFactory(properties);
    initOutboundBridgeManager(properties);
    initInboundBridgeManager(properties);
  }

  protected void initArtemisServerLocator(Map<String, String> properties) {
    artemisServerLocator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(InVMConnectorFactory.class.getName()));

    artemisServerLocator.setInitialConnectAttempts(-1);
    artemisServerLocator.setReconnectAttempts(-1);
    artemisServerLocator.setRetryInterval(1000L);
    artemisServerLocator.setRetryIntervalMultiplier(1.5);
    artemisServerLocator.setMaxRetryInterval(30000L);
  }

  protected void initKafkaClientFactory(Map<String, String> properties) {
    kafkaClientFactory = new DefaultKafkaClientFactory();

    // TODO
  }

  protected void initOutboundBridgeManager(Map<String, String> properties) {
    outboundBridgeManager = new OutboundBridgeManager();

    String rawArtemisOutboundAddresses = properties.getOrDefault(OUTBOUND_ADDRESSES, DEFAULT_OUTBOUND_ADDRESSES);
    outboundBridgeManager.getArtemisOutboundAddresses().addAll(Arrays.asList(rawArtemisOutboundAddresses.split(DEFAULT_ADDRESS_SPLIT_REGEX)));
  }

  protected void initInboundBridgeManager(Map<String, String> properties) {
    inboundBridgeManager = new InboundBridgeManager();

    String rawArtemisOutboundAddresses = properties.getOrDefault(OUTBOUND_ADDRESSES, DEFAULT_OUTBOUND_ADDRESSES);
    inboundBridgeManager.getArtemisOutboundAddresses().addAll(Arrays.asList(rawArtemisOutboundAddresses.split(DEFAULT_ADDRESS_SPLIT_REGEX)));
    String rawArtemisInboundAddresses = properties.get(INBOUND_ADDRESSES);
    if (rawArtemisInboundAddresses != null && !rawArtemisInboundAddresses.isBlank()) {
      inboundBridgeManager.getArtemisInboundAddresses().addAll(Arrays.asList(rawArtemisInboundAddresses.split(DEFAULT_ADDRESS_SPLIT_REGEX)));
    }
  }

  @Override
  public void registered(ActiveMQServer server) {
    log.debug("{} plugin registered.", this.getClass().getSimpleName());

    this.server = server;

    CompletableFuture.runAsync(() -> {
      await().until(() -> server.isStarted(), is(true));

      try {
        artemisConnection = artemisServerLocator.createSessionFactory();
      } catch (Exception e) {
        log.error("Unable to create connection to broker.");
        log.debug("Stack trace:", e);
      }

      if (artemisConnection != null) {
        try {
          outboundBridgeManager.setArtemisServer(server);
          outboundBridgeManager.setArtemisConnection(artemisConnection);
          outboundBridgeManager.setKafkaProducer(kafkaClientFactory.kafkaProducer());
          outboundBridgeManager.start();
        } catch (Exception e) {
          log.error("Unable to start outbound bridge manager.");
          log.debug("Stack trace:", e);
        }

        try {
          inboundBridgeManager.setArtemisServer(server);
          inboundBridgeManager.setArtemisConnection(artemisConnection);
          inboundBridgeManager.setKafkaConsumer(kafkaClientFactory.kafkaConsumer());
          inboundBridgeManager.start();
        } catch (Exception e) {
          log.error("Unable to start inbound bridge manager.");
          log.debug("Stack trace:", e);
        }
      }
    });
  }

  @Override
  public void unregistered(ActiveMQServer server) {
    log.debug("{} plugin unregistered.", this.getClass().getSimpleName());
  }

  @Override
  public void afterCreateConsumer(ServerConsumer consumer) throws ActiveMQException {
    log.debug("Consumer {} created for {}.", consumer.getID(), consumer.getQueueAddress().toString());
    inboundBridgeManager.onConsumerAdded(consumer);
  }

  @Override
  public void beforeCloseConsumer(ServerConsumer consumer, boolean failed) throws ActiveMQException {
    log.debug("Consumer {} closed for {}.", consumer.getID(), consumer.getQueueAddress().toString());
    inboundBridgeManager.onConsumerRemoved(consumer);
  }
}
