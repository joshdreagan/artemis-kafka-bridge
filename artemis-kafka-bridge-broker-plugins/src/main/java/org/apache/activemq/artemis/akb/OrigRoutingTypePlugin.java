package org.apache.activemq.artemis.akb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrigRoutingTypePlugin implements ActiveMQServerPlugin {

  private static final Logger log = LoggerFactory.getLogger(OrigRoutingTypePlugin.class);

  @Override
  public void init(Map<String, String> properties) {
    log.debug("{} plugin init.", this.getClass().getSimpleName());
  }

  @Override
  public void registered(ActiveMQServer server) {
    log.debug("{} plugin registered.", this.getClass().getSimpleName());
  }

  @Override
  public void unregistered(ActiveMQServer server) {
    log.debug("{} plugin unregistered.", this.getClass().getSimpleName());
  }

  @Override
  public void beforeSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue) throws ActiveMQException {
    SimpleString originalAddress = message.getAddressSimpleString();
    AddressInfo originalAddressInfo = session.getAddress(originalAddress);
    RoutingType originalRoutingType = originalAddressInfo.getRoutingType();
    if (originalRoutingType != null) {
      log.debug("Adding {}={} header to message {}.", Message.HDR_ORIG_ROUTING_TYPE, originalRoutingType.name(), message.getMessageID());
      message.putStringProperty(Message.HDR_ORIG_ROUTING_TYPE, originalRoutingType.name());
      message.reencode();
    }
  }
}
