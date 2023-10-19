package org.apache.activemq.artemis.akb;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

public enum AkbMessageType {
  TEXT,
  BYTES;
  
  public static AkbMessageType fromJmsMessage(Message jmsMessage) {
    if (jmsMessage instanceof TextMessage) {
      return TEXT;
    } else if (jmsMessage instanceof BytesMessage) {
      return BYTES;
    } else {
      throw new IllegalArgumentException(String.format("Unable to convert AkbMessageType. Unknown jmsMessage type: %s", jmsMessage.getClass()));
    }
  }
}
