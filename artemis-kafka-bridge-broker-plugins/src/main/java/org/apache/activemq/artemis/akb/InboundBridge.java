package org.apache.activemq.artemis.akb;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InboundBridge {

  private static final Logger log = LoggerFactory.getLogger(InboundBridge.class);

  private final String artemisInboundAddress;
  private final ClientSessionFactory artemisConnection;
  private final Consumer kafkaConsumer;

  private ClientSession artemisSession;
  private boolean running;
  private boolean closed;

  public InboundBridge(String artemisInboundAddress, ClientSessionFactory artemisConnection, Consumer kafkaConsumer) {
    this.artemisInboundAddress = artemisInboundAddress;
    this.artemisConnection = artemisConnection;
    this.kafkaConsumer = kafkaConsumer;
  }

  public String getArtemisInboundAddress() {
    checkClosed();
    return artemisInboundAddress;
  }

  public ClientSessionFactory getArtemisConnection() {
    checkClosed();
    return artemisConnection;
  }

  public Consumer getKafkaConsumer() {
    checkClosed();
    return kafkaConsumer;
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isClosed() {
    return closed;
  }

  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("This inbound bridge is closed.");
    }
  }

  public void start() {
    checkClosed();
    if (!running) {
      log.debug("Starting inbound bridge: {}", artemisInboundAddress);
      try {
        if (artemisSession == null || artemisSession.isClosed()) {
          artemisSession = artemisConnection.createSession();
          ClientProducer artemisProducer = artemisSession.createProducer(artemisInboundAddress + ".inbound");
          try {
          kafkaConsumer.subscribe(Collections.singleton(artemisInboundAddress));
          while (true) {
            ConsumerRecords<byte[], byte[]> kafkaRecords = kafkaConsumer.poll(Duration.ofMillis(5000L));
            for (ConsumerRecord<byte[], byte[]> kafkaRecord : kafkaRecords) {
              ClientMessage artemisMessage = artemisSession.createMessage(true);
              String kafkaTopic = kafkaRecord.topic();
              byte[] kafkaBody = kafkaRecord.value();
              artemisMessage.getBodyBuffer().writeBytes(kafkaBody);
              for (Header header : kafkaRecord.headers()) {
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
                }
              }
              artemisProducer.send(artemisMessage);
            }
          }
          } catch (WakeupException e) {
            log.debug("Got a wakeup");
          } catch (RuntimeException e) {
            log.error("Got an error processing message.", e);
          }
        }

        artemisSession.start();
        running = true;
      } catch (ActiveMQException e) {
        log.error("Unable to start the inbound bridge: {}", artemisInboundAddress);
        throw new RuntimeException(e);
      }
    }
  }

  public void stop() {
    checkClosed();
    if (running) {
      log.debug("Stopping inbound bridge: {}", artemisInboundAddress);
      boolean stopped = true;
      if (artemisSession != null) {
        try {
          artemisSession.stop();
        } catch (ActiveMQException e) {
          log.error("Unable to stop artemis session.");
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
      } finally {
        artemisSession = null;
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
    closed = true;
  }
}
