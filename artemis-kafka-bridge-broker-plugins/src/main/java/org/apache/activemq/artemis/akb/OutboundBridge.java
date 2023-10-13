package org.apache.activemq.artemis.akb;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
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
  private final Producer kafkaProducer;

  private ClientSession artemisSession;
  private boolean running;
  private boolean closed;

  public OutboundBridge(String artemisOutboundAddress, ClientSessionFactory artemisConnection, Producer kafkaProducer) {
    this.artemisOutboundAddress = artemisOutboundAddress;
    this.artemisConnection = artemisConnection;
    this.kafkaProducer = kafkaProducer;
  }

  public String getArtemisOutboundAddress() {
    checkClosed();
    return artemisOutboundAddress;
  }

  public ClientSessionFactory getArtemisConnection() {
    checkClosed();
    return artemisConnection;
  }

  public Producer getKafkaProducer() {
    checkClosed();
    return kafkaProducer;
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isClosed() {
    return closed;
  }

  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("This outbound bridge is closed.");
    }
  }

  public void start() {
    checkClosed();
    if (!running) {
      log.debug("Starting outbound bridge: {}", artemisOutboundAddress);
      try {
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
  }

  public void stop() {
    checkClosed();
    if (running) {
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
  }

  public void restart() {
    checkClosed();
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
