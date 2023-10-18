package org.apache.activemq.artemis.akb.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerRecordPoller<K, V> {

  private static final Logger log = LoggerFactory.getLogger(ConsumerRecordPoller.class);

  private final ClientFactory kafkaClientFactory;
  private final Collection<String> topics;
  private final ConsumerRecordHandler<K, V> consumerRecordHandler;

  private ExecutorService poller;

  private boolean running = false;
  private boolean closed = false;

  private boolean stopRequested = false;

  public ConsumerRecordPoller(ClientFactory kafkaClientFactory, Collection<String> topics, ConsumerRecordHandler<K, V> consumerRecordHandler) {
    this.kafkaClientFactory = Objects.requireNonNull(kafkaClientFactory, "The kafkaClientFactory parameter must not be null.");
    this.topics = Objects.requireNonNull(topics, "The topics parameter must not be null.");
    this.consumerRecordHandler = Objects.requireNonNull(consumerRecordHandler, "The consumerRecordHandler parameter must not be null.");
  }

  public ClientFactory getKafkaClientFactory() {
    return kafkaClientFactory;
  }

  public Collection<String> getTopics() {
    return topics;
  }

  public ConsumerRecordHandler<K, V> getConsumerRecordHandler() {
    return consumerRecordHandler;
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isClosed() {
    return closed;
  }

  private void throwIfClosed() {
    if (closed) {
      throw new IllegalStateException("This consumer record poller is closed.");
    }
  }

  public void start() {
    throwIfClosed();
    if (running) {
      return;
    }

    if (poller == null) {
      poller = Executors.newSingleThreadExecutor();
    }
    stopRequested = false;
    poller.submit(() -> {
      Consumer consumer = consumer = kafkaClientFactory.createKafkaConsumer();
      consumer.subscribe(topics);
      while (!stopRequested) {
        try {
          ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofMillis(5000L));
          log.debug("Polled a batch of {} kafka consumer records.", consumerRecords.count());
          if (consumerRecords.count() == 0) {
            if (!stopRequested) {
              Thread.sleep(1000L);
            }
          } else {
            consumerRecords.forEach((consumerRecord) -> {
              consumerRecordHandler.onConsumerRecord(consumerRecord);
            });
            consumer.commitSync();
          }
        } catch (Exception e) {
          log.error("An error occurred polling/processing consumer records.");
          log.debug("Stack trace", e);
        }
      }
      try {
        consumer.unsubscribe();
        consumer.close();
      } catch (Exception e) {
        log.error("An error occurred closing kafka consumer.");
        log.debug("Stack trace", e);
      }
    });
    running = true;
  }

  public void stop() {
    throwIfClosed();
    if (!running) {
      return;
    }

    stopRequested = true;
    if (poller != null) {
      poller.shutdown();
    }
    poller = null;
    running = false;
  }

  public void close() {
    stop();
    closed = true;
  }
}
