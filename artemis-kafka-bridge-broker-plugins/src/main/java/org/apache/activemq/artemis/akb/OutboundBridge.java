package org.apache.activemq.artemis.akb;

import java.nio.charset.StandardCharsets;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundBridge {

  private static final Logger log = LoggerFactory.getLogger(OutboundBridge.class);

  private final String artemisOutboundAddress;
  private final OutboundBridgeManager outboundBridgeManager;

  private ClientSession artemisSession;
  private boolean running;

  public OutboundBridge(String artemisOutboundAddress, OutboundBridgeManager outboudBridgeManager) {
    this.artemisOutboundAddress = artemisOutboundAddress;
    this.outboundBridgeManager = outboudBridgeManager;
  }

  public void start() {
    if (!running) {
      log.debug("Starting outbound bridge: {}", artemisOutboundAddress);
      try {
        artemisSession = outboundBridgeManager.getArtemisConnection().createSession();
        
        ClientConsumer artemisConsumer = artemisSession.createConsumer(artemisOutboundAddress);
        artemisConsumer.setMessageHandler(new OutboundMessageHandler());

        artemisSession.start();
        running = true;
      } catch (ActiveMQException e) {
        log.error("Unable to start the outbound bridge: {}", artemisOutboundAddress);
        throw new RuntimeException(e);
      }
    }
  }

  public void stop() {
    if (running) {
      log.debug("Stopping outbound bridge: {}", artemisOutboundAddress);
      try {
        if (artemisSession != null) {
          artemisSession.stop();
          artemisSession.close();
        }
        running = false;
      } catch (ActiveMQException e) {
        log.error("Unable to stop the outbound bridge: {}", artemisOutboundAddress);
        throw new RuntimeException(e);
      } finally {
        artemisSession = null;
      }
    }
  }

  public void restart() {
    stop();
    start();
  }

  private class OutboundMessageHandler implements MessageHandler {

    @Override
    public void onMessage(ClientMessage artemisMessage) {
      try {
        String artemisMessageId = artemisMessage.getStringProperty(ClientMessage.HDR_ORIG_MESSAGE_ID);
        String artemisDestinationName = artemisMessage.getStringProperty(ClientMessage.HDR_ORIGINAL_ADDRESS);
        String artemisRoutingType = artemisMessage.getStringProperty(ClientMessage.HDR_ORIG_ROUTING_TYPE);

        byte[] kafkaMessageBody;
        if (artemisMessage.isLargeMessage()) {
          throw new RuntimeException("Unable to handle large messages at this time.");
        } else {
          ActiveMQBuffer artemisMessageBody = artemisMessage.getReadOnlyBodyBuffer();
          kafkaMessageBody = new byte[artemisMessageBody.readableBytes()];
          artemisMessageBody.readBytes(kafkaMessageBody);
        }

        ProducerRecord kafkaMessage = new ProducerRecord(artemisDestinationName, kafkaMessageBody);
        kafkaMessage.headers().add("AkbMessageId", artemisMessageId.getBytes(StandardCharsets.UTF_8));
        kafkaMessage.headers().add("AkbDestinationName", artemisDestinationName.getBytes(StandardCharsets.UTF_8));
        kafkaMessage.headers().add("AkbRoutingType", artemisRoutingType.getBytes(StandardCharsets.UTF_8));
        outboundBridgeManager.getKafkaProducer().send(kafkaMessage);
      } catch (Exception e) {
        log.error("Unable to process message: {}", artemisMessage.getMessageID());
        log.debug("Stack trace:", e);
      }
    }
  }
}
