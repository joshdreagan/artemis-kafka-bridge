package org.apache.activemq.artemis.akb;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import org.apache.activemq.artemis.akb.kafka.ClientFactory;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundBridge {

  private static final Logger log = LoggerFactory.getLogger(OutboundBridge.class);

  private final String artemisOutboundAddress;
  private final Connection artemisConnection;
  private final ClientFactory kafkaClientFactory;

  private Session artemisSession;
  private MessageConsumer artemisConsumer;
  private Producer kafkaProducer;

  private boolean running = false;
  private boolean closed = false;

  public OutboundBridge(String artemisOutboundAddress, Connection artemisConnection, ClientFactory kafkaClientFactory) {
    this.artemisOutboundAddress = Objects.requireNonNull(artemisOutboundAddress, "The artemisOutboundAddress parameter must not be null.");
    this.artemisConnection = Objects.requireNonNull(artemisConnection, "The artemisConnection parameter must not be null.");
    this.kafkaClientFactory = Objects.requireNonNull(kafkaClientFactory, "The kafkaClientFactory parameter must not be null.");
  }

  public String getArtemisOutboundAddress() {
    return artemisOutboundAddress;
  }

  public Connection getArtemisConnection() {
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
      if (artemisSession == null) {
        artemisSession = artemisConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        artemisConsumer = artemisSession.createConsumer(artemisSession.createQueue(artemisOutboundAddress));
        artemisConsumer.setMessageListener(new OutboundMessageHandler());
      }

      running = true;
    } catch (JMSException e) {
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
        artemisConsumer.close();
        artemisSession.close();
      } catch (JMSException e) {
        log.error("Unable to stop artemis session.");
        throw new RuntimeException(e);
      } finally {
        artemisConsumer = null;
        artemisSession = null;
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

  private class OutboundMessageHandler implements MessageListener {

    @Override
    public void onMessage(Message artemisMessage) {
      if (artemisMessage != null) {
        String artemisJmsMessageId = null;
        try {
          artemisJmsMessageId = artemisMessage.getJMSMessageID();

          String artemisMessageId = Objects.requireNonNull(artemisMessage.getStringProperty(ClientMessage.HDR_ORIG_MESSAGE_ID.toString()), String.format("The %s header must not be null.", ClientMessage.HDR_ORIG_MESSAGE_ID));
          AkbMessageType artemisMessageType = AkbMessageType.fromJmsMessage(artemisMessage);
          String artemisDestinationName = Objects.requireNonNull(artemisMessage.getStringProperty(ClientMessage.HDR_ORIGINAL_ADDRESS.toString()), String.format("The %s header must not be null.", ClientMessage.HDR_ORIGINAL_ADDRESS));
          RoutingType artemisRoutingType = RoutingType.getType(artemisMessage.getByteProperty(ClientMessage.HDR_ORIG_ROUTING_TYPE.toString()));
          String artemisGroupId = artemisMessage.getStringProperty(ClientMessage.HDR_GROUP_ID.toString());

          byte[] kafkaMessageBody = new byte[0];
          switch (artemisMessageType) {
            case TEXT -> {
              kafkaMessageBody = artemisMessage.getBody(String.class).getBytes(StandardCharsets.UTF_8);
            }
            case BYTES -> {
              kafkaMessageBody = artemisMessage.getBody(byte[].class);
            }
          }

          ProducerRecord kafkaMessage = null;
          if (artemisGroupId != null && !artemisGroupId.isBlank()) {
            kafkaMessage = new ProducerRecord(artemisDestinationName, artemisGroupId.getBytes(StandardCharsets.UTF_8), kafkaMessageBody);
          } else {
            kafkaMessage = new ProducerRecord(artemisDestinationName, kafkaMessageBody);
          }
          kafkaMessage.headers().add(AkbHeaders.HDR_AKB_MESSAGE_ID, artemisMessageId.getBytes(StandardCharsets.UTF_8));
          kafkaMessage.headers().add(AkbHeaders.HDR_AKB_MESSAGE_TYPE, artemisMessageType.name().getBytes(StandardCharsets.UTF_8));
          kafkaMessage.headers().add(AkbHeaders.HDR_AKB_DESTINATION_NAME, artemisDestinationName.getBytes(StandardCharsets.UTF_8));
          kafkaMessage.headers().add(AkbHeaders.HDR_AKB_ROUTING_TYPE, artemisRoutingType.name().getBytes(StandardCharsets.UTF_8));
          kafkaProducer.send(kafkaMessage);

          artemisMessage.acknowledge();
        } catch (Exception e) {
          log.error("Unable to process message: {}", artemisJmsMessageId);
          log.debug("Stack trace:", e);
        }
      } else {
        log.warn("Received a null artemis message.");
      }
    }
  }
}
