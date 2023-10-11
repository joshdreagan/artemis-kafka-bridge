package org.apache.activemq.artemis.akb;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InboundBridge {
  
  private static final Logger log = LoggerFactory.getLogger(InboundBridge.class);
  
  private final String artemisInboundAddress;
  private final InboundBridgeManager inboundBridgeManager;

  private ClientSession artemisSession;
  private boolean running;

  public InboundBridge(String artemisInboundAddress, InboundBridgeManager inboundBridgeManager) {
    this.artemisInboundAddress = artemisInboundAddress;
    this.inboundBridgeManager = inboundBridgeManager;
  }
  
  public void start() {
    if (!running) {
      log.debug("Starting inbound bridge: {}", artemisInboundAddress);
       try {
        artemisSession = inboundBridgeManager.getArtemisConnection().createSession();
        
        artemisSession.start();
        running = true;
      } catch (ActiveMQException e) {
        log.error("Unable to start the inbound bridge: {}", artemisInboundAddress);
        throw new RuntimeException(e);
      }
   }
  }
  
  public void stop() {
    if (running) {
      log.debug("Stopping inbound bridge: {}", artemisInboundAddress);
      try {
        if (artemisSession != null) {
          artemisSession.stop();
          artemisSession.close();
        }
        running = false;
      } catch (ActiveMQException e) {
        log.error("Unable to stop the inbound bridge: {}", artemisInboundAddress);
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
}
