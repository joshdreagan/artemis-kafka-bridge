package org.apache.activemq.artemis.akb;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArtemisKafkaBridgePlugin implements ActiveMQServerPlugin {

  private static final Logger log = LoggerFactory.getLogger(ArtemisKafkaBridgePlugin.class);

  public static final String ARTEMIS_OUTBOUND_ADDRESSES = "artemis.outbound-addresses";
  public static final String ARTEMIS_INBOUND_ADDRESS_INCLUDES = "artemis.inbound-address-includes";
  public static final String ARTEMIS_INBOUND_ADDRESS_EXCLUDES = "artemis.inbound-address-excludes";
  public static final String ARTEMIS_INITIAL_CONNECT_ATTEMPTS = "artemis.initial-connect-attempts";
  public static final String ARTEMIS_RECONNECT_ATTEMPTS = "artemis.reconnect-attempts";
  public static final String ARTEMIS_RETRY_INTERVAL = "artemis.retry-interval";
  public static final String ARTEMIS_RETRY_INTERVAL_MULTIPLIER = "artemis.retry-interval-multiplier";
  public static final String ARTEMIS_MAX_RETRY_INTERVAL = "artemis.max-retry-interval";
  public static final String ARTEMIS_CALL_TIMEOUT = "artemis.call-timout";

  public static final String DEFAULT_ARTEMIS_OUTBOUND_ADDRESSES = "__akb.outbound";
  public static final String DEFAULT_ARTEMIS_INITIAL_CONNECT_ATTEMPTS = "-1";
  public static final String DEFAULT_ARTEMIS_RECONNECT_ATTEMPTS = "-1";
  public static final String DEFAULT_ARTEMIS_RETRY_INTERVAL = "1000";
  public static final String DEFAULT_ARTEMIS_RETRY_INTERVAL_MULTIPLIER = "1.5";
  public static final String DEFAULT_ARTEMIS_MAX_RETRY_INTERVAL = "30000";
  public static final String DEFAULT_ARTEMIS_CALL_TIMEOUT = "5000";

  public static final String DEFAULT_ADDRESS_SPLIT_REGEX = "\\s*[,:;\\s]\\s*";

  protected ServerLocator artemisConnectionFactory;
  protected KafkaClientFactory kafkaClientFactory;
  protected OutboundBridgeManager outboundBridgeManager;
  protected InboundBridgeManager inboundBridgeManager;

  protected ActiveMQServer server;

  @Override
  public void init(Map<String, String> properties) {
    log.debug("{} plugin init.", this.getClass().getSimpleName());
    initArtemisConnectionFactory(properties);
    initKafkaClientFactory(properties);
    initOutboundBridgeManager(properties);
    initInboundBridgeManager(properties);
  }

  protected void initArtemisConnectionFactory(Map<String, String> properties) {
    artemisConnectionFactory = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(InVMConnectorFactory.class.getName()));

    artemisConnectionFactory.setCallTimeout(Long.parseLong(properties.getOrDefault(ARTEMIS_CALL_TIMEOUT, DEFAULT_ARTEMIS_CALL_TIMEOUT)));
    artemisConnectionFactory.setInitialConnectAttempts(Integer.parseInt(properties.getOrDefault(ARTEMIS_INITIAL_CONNECT_ATTEMPTS, DEFAULT_ARTEMIS_INITIAL_CONNECT_ATTEMPTS)));
    artemisConnectionFactory.setReconnectAttempts(Integer.parseInt(properties.getOrDefault(ARTEMIS_RECONNECT_ATTEMPTS, DEFAULT_ARTEMIS_RECONNECT_ATTEMPTS)));
    artemisConnectionFactory.setRetryInterval(Long.parseLong(properties.getOrDefault(ARTEMIS_RETRY_INTERVAL, DEFAULT_ARTEMIS_RETRY_INTERVAL)));
    artemisConnectionFactory.setRetryIntervalMultiplier(Double.parseDouble(properties.getOrDefault(ARTEMIS_RETRY_INTERVAL_MULTIPLIER, DEFAULT_ARTEMIS_RETRY_INTERVAL_MULTIPLIER)));
    artemisConnectionFactory.setMaxRetryInterval(Long.parseLong(properties.getOrDefault(ARTEMIS_MAX_RETRY_INTERVAL, DEFAULT_ARTEMIS_MAX_RETRY_INTERVAL)));
  }

  protected void initKafkaClientFactory(Map<String, String> properties) {
    kafkaClientFactory = new DefaultKafkaClientFactory();

    // TODO
  }

  protected void initOutboundBridgeManager(Map<String, String> properties) {
    outboundBridgeManager = new OutboundBridgeManager();

    String rawArtemisOutboundAddresses = properties.getOrDefault(ARTEMIS_OUTBOUND_ADDRESSES, DEFAULT_ARTEMIS_OUTBOUND_ADDRESSES);
    outboundBridgeManager.getArtemisOutboundAddresses().addAll(Arrays.asList(rawArtemisOutboundAddresses.split(DEFAULT_ADDRESS_SPLIT_REGEX)));
  }

  protected void initInboundBridgeManager(Map<String, String> properties) {
    inboundBridgeManager = new InboundBridgeManager();

    String artemisOutboundAddresses = properties.getOrDefault(ARTEMIS_OUTBOUND_ADDRESSES, DEFAULT_ARTEMIS_OUTBOUND_ADDRESSES);
    inboundBridgeManager.getArtemisInboundAddressExcludes().addAll(Arrays.asList(artemisOutboundAddresses.split(DEFAULT_ADDRESS_SPLIT_REGEX)));

    String artemisInboundAddressExcludes = properties.get(ARTEMIS_INBOUND_ADDRESS_EXCLUDES);
    if (artemisInboundAddressExcludes != null) {
      inboundBridgeManager.getArtemisInboundAddressExcludes().addAll(Arrays.asList(artemisInboundAddressExcludes.split(DEFAULT_ADDRESS_SPLIT_REGEX)));
    }

    String artemisInboundAddressIncludes = properties.get(ARTEMIS_INBOUND_ADDRESS_INCLUDES);
    if (artemisInboundAddressIncludes != null) {
      inboundBridgeManager.getArtemisInboundAddressIncludes().addAll(Arrays.asList(artemisInboundAddressIncludes.split(DEFAULT_ADDRESS_SPLIT_REGEX)));
    }
  }

  @Override
  public void registered(ActiveMQServer server) {
    log.debug("{} plugin registered.", this.getClass().getSimpleName());

    this.server = server;

    outboundBridgeManager.setArtemisServer(server);
    outboundBridgeManager.setArtemisConnectionFactory(artemisConnectionFactory);
    outboundBridgeManager.setKafkaClientFactory(kafkaClientFactory);

    inboundBridgeManager.setArtemisServer(server);
    inboundBridgeManager.setArtemisConnectionFactory(artemisConnectionFactory);
    inboundBridgeManager.setKafkaClientFactory(kafkaClientFactory);

    CompletableFuture.runAsync(() -> {
      while (server.getState() != ActiveMQServer.SERVER_STATE.STARTED) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
        }
      }
      log.debug("Artemis server is running. Starting bridge managers.");

      try {
        outboundBridgeManager.start();
      } catch (Exception e) {
        log.error("Unable to start outbound bridge manager.");
        log.debug("Stack trace:", e);
      }

      try {
        inboundBridgeManager.start();
      } catch (Exception e) {
        log.error("Unable to start inbound bridge manager.");
        log.debug("Stack trace:", e);
      }
    });

    CompletableFuture.runAsync(() -> {
      while (server.getState() != ActiveMQServer.SERVER_STATE.STOPPING && server.getState() != ActiveMQServer.SERVER_STATE.STOPPED) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
        }
      }
      log.debug("Artemis server is shutting down. Stopping bridge managers.");

      if (inboundBridgeManager != null) {
        try {
          inboundBridgeManager.stop();
          inboundBridgeManager.close();
        } catch (Exception e) {
          log.error("Unable to close inbound bridge manager.");
          log.debug("Stack trace:", e);
        } finally {
          inboundBridgeManager = null;
        }
      }
      if (outboundBridgeManager != null) {
        try {
          outboundBridgeManager.stop();
          outboundBridgeManager.close();
        } catch (Exception e) {
          log.error("Unable to close outbound bridge manager.");
          log.debug("Stack trace:", e);
        } finally {
          outboundBridgeManager = null;
        }
      }
    });

  }

  @Override
  public void unregistered(ActiveMQServer server) {
    log.debug("{} plugin unregistered.", this.getClass().getSimpleName());
    if (inboundBridgeManager != null) {
      try {
        inboundBridgeManager.stop();
      } catch (Exception e) {
        log.error("Unable to stop inbound bridge manager.");
        log.debug("Stack trace:", e);
      }
    }
    if (outboundBridgeManager != null) {
      try {
        outboundBridgeManager.stop();
      } catch (Exception e) {
        log.error("Unable to stop outbound bridge manager.");
        log.debug("Stack trace:", e);
      }
    }
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
