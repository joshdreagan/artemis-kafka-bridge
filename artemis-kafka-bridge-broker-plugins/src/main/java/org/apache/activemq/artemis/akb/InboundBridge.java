package org.apache.activemq.artemis.akb;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import org.apache.activemq.artemis.akb.kafka.ClientFactory;
import org.apache.activemq.artemis.akb.kafka.ConsumerRecordHandler;
import org.apache.activemq.artemis.akb.kafka.ConsumerRecordPoller;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InboundBridge {

  private static final Logger log = LoggerFactory.getLogger(InboundBridge.class);

  private final String artemisInboundAddress;
  private final ClientSessionFactory artemisConnection;
  private final String kafkaInboundAddress;
  private final ClientFactory kafkaClientFactory;

  private ClientSession artemisSession;
  private ClientProducer artemisProducer;
  private ConsumerRecordPoller kafkaConsumerRecordPoller;
  private Consumer kafkaConsumer;
  
  private boolean running = false;
  private boolean closed = false;

  public InboundBridge(String artemisInboundAddress, ClientSessionFactory artemisConnection, String kafkaInboundAddress, ClientFactory kafkaClientFactory) {
    this.artemisInboundAddress = Objects.requireNonNull(artemisInboundAddress, "The artemisInboundAddress parameter must not be null.");
    this.artemisConnection = Objects.requireNonNull(artemisConnection, "The artemisConnection parameter must not be null.");
    this.kafkaInboundAddress = Objects.requireNonNull(kafkaInboundAddress, "The kafkaInboundAddress parameter must not be null.");
    this.kafkaClientFactory = Objects.requireNonNull(kafkaClientFactory, "The kafkaClientFactory parameter must not be null.");
  }

  public String getArtemisInboundAddress() {
    return artemisInboundAddress;
  }

  public ClientSessionFactory getArtemisConnection() {
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
      if (artemisSession == null || artemisSession.isClosed()) {
        artemisSession = artemisConnection.createSession();
        artemisProducer = artemisSession.createProducer(artemisInboundAddress);
      }
      artemisSession.start();

      if (kafkaConsumer == null) {
        kafkaConsumer = kafkaClientFactory.createKafkaConsumer();
      }
      kafkaConsumer.subscribe(Collections.singleton(kafkaInboundAddress));
      if (kafkaConsumerRecordPoller == null) {
        kafkaConsumerRecordPoller = new ConsumerRecordPoller<>(kafkaConsumer, new InboundConsumerRecordHandler());
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
    if (kafkaConsumer != null) {
      try {
        kafkaConsumer.unsubscribe();
      } catch (Exception e) {
        log.error("Unable to unsubscribe kafka consumer.");
        throw new RuntimeException(e);
      }
    }
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
    if (kafkaConsumerRecordPoller != null) {
      try {
        kafkaConsumerRecordPoller.close();
      } catch (Exception e) {
        log.error("Unable to stop kafka consumer record poller.");
        log.debug("Stack trace:", e);
      }
    }
    if (kafkaConsumer != null) {
      try {
        kafkaConsumer.close();
      } catch (Exception e) {
        log.error("Unable to close kafka consumer.");
        log.debug("Stack trace:", e);
      }
    }
    if (artemisSession != null) {
      try {
        artemisSession.close();
      } catch (ActiveMQException e) {
        log.error("Unable to close artemis session.");
      } finally {
        artemisProducer = null;
        artemisSession = null;
      }
    }
    closed = true;
  }

  private class InboundConsumerRecordHandler implements ConsumerRecordHandler<byte[], byte[]> {

    @Override
    public void onConsumerRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
      try {
        ClientMessage artemisMessage = artemisSession.createMessage(true);
        String kafkaTopic = consumerRecord.topic();
        byte[] kafkaBody = consumerRecord.value();
        artemisMessage.getBodyBuffer().writeBytes(kafkaBody);
        for (Header header : consumerRecord.headers()) {
          String kafkaHeaderName = header.key();
          byte[] kafkaHeaderValue = header.value();
          switch (kafkaHeaderName) {
            case AkbHeaders.HDR_AKB_MESSAGE_ID -> {
              artemisMessage.putStringProperty(ClientMessage.HDR_ORIG_MESSAGE_ID, new String(kafkaHeaderValue, StandardCharsets.UTF_8));
            }
            case AkbHeaders.HDR_AKB_DESTINATION_NAME -> {
              artemisMessage.putStringProperty(ClientMessage.HDR_ORIGINAL_ADDRESS, new String(kafkaHeaderValue, StandardCharsets.UTF_8));
            }
            case AkbHeaders.HDR_AKB_ROUTING_TYPE -> {
              artemisMessage.putStringProperty(ClientMessage.HDR_ORIG_ROUTING_TYPE, new String(kafkaHeaderValue, StandardCharsets.UTF_8));
            }
            case AkbHeaders.HDR_AKB_GROUP_ID -> {
              artemisMessage.putStringProperty(ClientMessage.HDR_GROUP_ID, new String(kafkaHeaderValue, StandardCharsets.UTF_8));
            }
          }
        }
        artemisProducer.send(artemisMessage);
        log.debug("Inbound bridge processed message id: {}", artemisMessage.getStringProperty(ClientMessage.HDR_ORIG_MESSAGE_ID));
      } catch (Exception e) {
        log.error("Unable to process message: {}", consumerRecord);
      }
    }
  }
}
