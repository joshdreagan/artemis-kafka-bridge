package org.apache.activemq.artemis.akb;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.artemis.akb.kafka.ClientFactory;
import org.apache.activemq.artemis.akb.kafka.ConsumerRecordHandler;
import org.apache.activemq.artemis.akb.kafka.ConsumerRecordPoller;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.api.core.RoutingType.MULTICAST;

public class InboundBridge {

  private static final Logger log = LoggerFactory.getLogger(InboundBridge.class);

  private final String artemisInboundAddress;
  private final Connection artemisConnection;
  private final String kafkaInboundAddress;
  private final ClientFactory kafkaClientFactory;

  private Session artemisSession;
  private MessageProducer artemisProducer;
  private ConsumerRecordPoller kafkaConsumerRecordPoller;

  private boolean running = false;
  private boolean closed = false;

  public InboundBridge(String artemisInboundAddress, Connection artemisConnection, String kafkaInboundAddress, ClientFactory kafkaClientFactory) {
    this.artemisInboundAddress = Objects.requireNonNull(artemisInboundAddress, "The artemisInboundAddress parameter must not be null.");
    this.artemisConnection = Objects.requireNonNull(artemisConnection, "The artemisConnection parameter must not be null.");
    this.kafkaInboundAddress = Objects.requireNonNull(kafkaInboundAddress, "The kafkaInboundAddress parameter must not be null.");
    this.kafkaClientFactory = Objects.requireNonNull(kafkaClientFactory, "The kafkaClientFactory parameter must not be null.");
  }

  public String getArtemisInboundAddress() {
    return artemisInboundAddress;
  }

  public Connection getArtemisConnection() {
    return artemisConnection;
  }

  public String getKafkaInboundAddress() {
    return kafkaInboundAddress;
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
      throw new IllegalStateException("This inbound bridge is closed.");
    }
  }

  public void start() {
    throwIfClosed();
    if (running) {
      return;
    }

    log.debug("Starting inbound bridge: {}", artemisInboundAddress);
    try {
      if (artemisSession == null) {
        artemisSession = artemisConnection.createSession();
        artemisProducer = artemisSession.createProducer(null);
      }

      if (kafkaConsumerRecordPoller == null) {
        kafkaConsumerRecordPoller = new ConsumerRecordPoller<>(kafkaClientFactory, Collections.singleton(kafkaInboundAddress), new InboundConsumerRecordHandler());
      }
      kafkaConsumerRecordPoller.start();
      running = true;
    } catch (Exception e) {
      log.error("Unable to start the inbound bridge: {}", artemisInboundAddress);
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    throwIfClosed();
    if (!running) {
      return;
    }

    log.debug("Stopping inbound bridge: {}", artemisInboundAddress);
    if (kafkaConsumerRecordPoller != null) {
      try {
        kafkaConsumerRecordPoller.stop();
      } catch (Exception e) {
        log.error("Unable to stop kafka consumer record poller.");
        throw new RuntimeException(e);
      }
    }
    if (artemisSession != null) {
      try {
        artemisProducer.close();
        artemisSession.close();
      } catch (JMSException e) {
        log.error("Unable to stop artemis session.");
        throw new RuntimeException(e);
      } finally {
        artemisProducer = null;
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
    if (kafkaConsumerRecordPoller != null) {
      try {
        kafkaConsumerRecordPoller.close();
      } catch (Exception e) {
        log.error("Unable to stop kafka consumer record poller.");
        log.debug("Stack trace:", e);
      }
    }
    closed = true;
  }

  private class InboundConsumerRecordHandler implements ConsumerRecordHandler<byte[], byte[]> {

    @Override
    public void onConsumerRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
      try {
        String artemisMessageId = new String(Objects.requireNonNull(consumerRecord.headers().lastHeader(AkbHeaders.HDR_AKB_MESSAGE_ID), String.format("The %s header must not be null.", AkbHeaders.HDR_AKB_MESSAGE_ID)).value(), StandardCharsets.UTF_8);
        AkbMessageType artemisMessageType = AkbMessageType.valueOf(new String(Objects.requireNonNull(consumerRecord.headers().lastHeader(AkbHeaders.HDR_AKB_MESSAGE_TYPE), String.format("The %s header must not be null.", AkbHeaders.HDR_AKB_MESSAGE_TYPE)).value(), StandardCharsets.UTF_8));
        String artemisDestinationName = new String(Objects.requireNonNull(consumerRecord.headers().lastHeader(AkbHeaders.HDR_AKB_DESTINATION_NAME), String.format("The %s header must not be null.", AkbHeaders.HDR_AKB_DESTINATION_NAME)).value(), StandardCharsets.UTF_8);
        RoutingType artemisRoutingType = RoutingType.valueOf(new String(Objects.requireNonNull(consumerRecord.headers().lastHeader(AkbHeaders.HDR_AKB_ROUTING_TYPE), String.format("The %s header must not be null.", AkbHeaders.HDR_AKB_ROUTING_TYPE)).value(), StandardCharsets.UTF_8));
        String artemisGroupId = null;
        byte[] kafkaKey = consumerRecord.key();
        if (kafkaKey != null) {
          artemisGroupId = new String(kafkaKey, StandardCharsets.UTF_8);
        }
        byte[] kafkaBody = consumerRecord.value();

        Message artemisMessage = null;
        switch (artemisMessageType) {
          case TEXT -> {
            artemisMessage = artemisSession.createTextMessage();
            ((TextMessage) artemisMessage).setText(new String(kafkaBody, StandardCharsets.UTF_8));
          }
          case BYTES -> {
            artemisMessage = artemisSession.createBytesMessage();
            ((BytesMessage) artemisMessage).writeBytes(kafkaBody);
          }
        }
        artemisMessage.setStringProperty(ClientMessage.HDR_ORIG_MESSAGE_ID.toString(), artemisMessageId);
        artemisMessage.setStringProperty(ClientMessage.HDR_ORIGINAL_ADDRESS.toString(), artemisDestinationName);
        artemisMessage.setStringProperty(ClientMessage.HDR_ORIG_ROUTING_TYPE.toString(), artemisRoutingType.name());
        artemisMessage.setStringProperty(ClientMessage.HDR_GROUP_ID.toString(), artemisGroupId);
        Destination destination = null;
        switch (artemisRoutingType) {
          case MULTICAST:
            destination = artemisSession.createTopic(artemisInboundAddress);
            break;
          case ANYCAST:
          default:
            destination = artemisSession.createQueue(artemisInboundAddress);
        }
        artemisProducer.send(destination, artemisMessage);
        log.debug("Inbound bridge processed message id: {}", artemisMessageId);
      } catch (Exception e) {
        log.error("Unable to process message: {}", consumerRecord);
      }
    }
  }
}
