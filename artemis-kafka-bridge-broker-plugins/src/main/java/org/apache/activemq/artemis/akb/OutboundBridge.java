package org.apache.activemq.artemis.akb;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.activemq.artemis.akb.kafka.ClientFactory;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundBridge {

  private static final Logger log = LoggerFactory.getLogger(OutboundBridge.class);

  private final String artemisOutboundAddress;
  private final ClientSessionFactory artemisConnection;
  private final ClientFactory kafkaClientFactory;

  private ClientSession artemisSession;
  private Producer kafkaProducer;

  private boolean running = false;
  private boolean closed = false;

  public OutboundBridge(String artemisOutboundAddress, ClientSessionFactory artemisConnection, ClientFactory kafkaClientFactory) {
    this.artemisOutboundAddress = Objects.requireNonNull(artemisOutboundAddress, "The artemisOutboundAddress parameter must not be null.");
    this.artemisConnection = Objects.requireNonNull(artemisConnection, "The artemisConnection parameter must not be null.");
    this.kafkaClientFactory = Objects.requireNonNull(kafkaClientFactory, "The kafkaClientFactory parameter must not be null.");
  }

  public String getArtemisOutboundAddress() {
    return artemisOutboundAddress;
  }

  public ClientSessionFactory getArtemisConnection() {
    return artemisConnection;
  }

  public ClientFactory getKafkaClientFactory() {
    return kafkaClientFactory;
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isClosed() {
    return closed;
  }

  private void throwIfClosed() {
    if (closed) {
      throw new IllegalStateException("This outbound bridge is closed.");
    }
  }

  public void start() {
    throwIfClosed();
    if (running) {
      return;
    }

    log.debug("Starting outbound bridge: {}", artemisOutboundAddress);
    try {
      if (kafkaProducer == null) {
        kafkaProducer = kafkaClientFactory.createKafkaProducer();
      }
      if (artemisSession == null || artemisSession.isClosed()) {
        artemisSession = artemisConnection.createSession();

        ClientConsumer artemisConsumer = artemisSession.createConsumer(artemisOutboundAddress);
        artemisConsumer.setMessageHandler(new OutboundMessageHandler());
      }
      artemisSession.start();
      
      running = true;
    } catch (ActiveMQException e) {
      log.error("Unable to start the outbound bridge: {}", artemisOutboundAddress);
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    throwIfClosed();
    if (!running) {
      return;
    }

    log.debug("Stopping outbound bridge: {}", artemisOutboundAddress);
    if (artemisSession != null) {
      try {
        artemisSession.stop();
      } catch (ActiveMQException e) {
        log.error("Unable to stop artemis session.");
        throw new RuntimeException(e);
      }
    }
    running = false;
  }

  public void restart() {
    throwIfClosed();
    stop();
    start();
  }

  public void close() {
    stop();
    if (artemisSession != null) {
      try {
        artemisSession.close();
      } catch (ActiveMQException e) {
        log.error("Unable to close artemis session.");
        log.debug("Stack trace:", e);
      } finally {
        artemisSession = null;
      }
    }
    if (kafkaProducer != null) {
      try {
        kafkaProducer.close();
      } catch (Exception e) {
        log.error("Unable to close kafka producer.");
        log.debug("Stack trace:", e);
      } finally {
        kafkaProducer = null;
      }
    }
    closed = true;
  }

  private class OutboundMessageHandler implements MessageHandler {

    @Override
    public void onMessage(ClientMessage artemisMessage) {
      if (artemisMessage != null) {
        try {
          String artemisMessageId = Objects.requireNonNull(artemisMessage.getStringProperty(ClientMessage.HDR_ORIG_MESSAGE_ID), String.format("The %s header must not be null.", ClientMessage.HDR_ORIG_MESSAGE_ID));
          String artemisDestinationName = Objects.requireNonNull(artemisMessage.getStringProperty(ClientMessage.HDR_ORIGINAL_ADDRESS), String.format("The %s header must not be null.", ClientMessage.HDR_ORIGINAL_ADDRESS));
          String artemisRoutingType = Objects.requireNonNull(artemisMessage.getStringProperty(ClientMessage.HDR_ORIG_ROUTING_TYPE), String.format("The %s header must not be null.", ClientMessage.HDR_ORIG_ROUTING_TYPE));

          byte[] kafkaMessageBody;
          if (artemisMessage.isLargeMessage()) {
            throw new RuntimeException("Unable to handle large messages at this time.");
          } else {
            ActiveMQBuffer artemisMessageBody = artemisMessage.getReadOnlyBodyBuffer();
            kafkaMessageBody = new byte[artemisMessageBody.readableBytes()];
            artemisMessageBody.readBytes(kafkaMessageBody);
          }

          ProducerRecord kafkaMessage = new ProducerRecord(artemisDestinationName, kafkaMessageBody);
          kafkaMessage.headers().add(AkbHeaders.HDR_AKB_MESSAGE_ID, artemisMessageId.getBytes(StandardCharsets.UTF_8));
          kafkaMessage.headers().add(AkbHeaders.HDR_AKB_DESTINATION_NAME, artemisDestinationName.getBytes(StandardCharsets.UTF_8));
          kafkaMessage.headers().add(AkbHeaders.HDR_AKB_ROUTING_TYPE, artemisRoutingType.getBytes(StandardCharsets.UTF_8));
          kafkaProducer.send(kafkaMessage);
        } catch (Exception e) {
          log.error("Unable to process message: {}", artemisMessage.getMessageID());
          log.debug("Stack trace:", e);
        }
      } else {
        log.warn("Received a null artemis message.");
      }
    }
  }
}
