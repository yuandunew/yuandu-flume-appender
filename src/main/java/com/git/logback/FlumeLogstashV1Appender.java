package com.git.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.*;

public class FlumeLogstashV1Appender extends UnsynchronizedAppenderBase<ILoggingEvent> {

  private static final int MAX_RECONNECTS = 3;
  private static final int MINIMUM_TIMEOUT = 1000;

  protected static final Charset UTF_8 = Charset.forName("UTF-8");

  private RpcClient client;

  private String flumeAgents;

  private String application;

  private String hostname;

  private String type;

  public String getApplication() {
    return application;
  }

  public void setApplication(String application) {
    this.application = application;
  }

  protected Layout<ILoggingEvent> layout;

  public Layout<ILoggingEvent> getLayout() {
    return layout;
  }

  public void setLayout(Layout<ILoggingEvent> layout) {
    this.layout = layout;
  }

  public void setFlumeAgents(String flumeAgents) {
    this.flumeAgents = flumeAgents;
  }

  @Override
  public void start() {
    if (layout == null) {
      addWarn("Layout was not defined, will only log the message, no stack traces or custom layout");
    }
    if(StringUtils.isEmpty(application)) {
      application = resolveApplication();
    }

    client = buildClient();

    super.start();

  }

  private RpcClient buildClient() {

    if(StringUtils.isNotEmpty(flumeAgents)) {
      String[] agentConfigs = flumeAgents.split(",");
      List<RemoteFlumeAgent> agents = new ArrayList<RemoteFlumeAgent>(agentConfigs.length);
      int totalAgents = 0;
      for(String conf: agentConfigs) {
        RemoteFlumeAgent agent = RemoteFlumeAgent.fromString(conf.trim());
        if( agent != null ) {
          agents.add(agent);
          totalAgents++;
        } else {
          addWarn("Cannot build a Flume agent config for '" + conf + "'");
        }
      }

      if(totalAgents > 0 ) {
        Properties props = buildFlumeProperties(agents);

        return RpcClientFactory.getInstance(props);
      } else {
        addError("No agents configured: '" + flumeAgents + "'");
      }
    } else {
      addError("flumeAgents property has not been defined");
    }

    return null;
  }

  private Properties buildFlumeProperties(List<RemoteFlumeAgent> agents) {
    Properties props = new Properties();

    props.put("client.type", "default_failover");

    int i=0;
    for(RemoteFlumeAgent agent: agents) {
      props.put("hosts.h" + (i++), agent.getHostname() + ':' + agent.getPort());
    }
    StringBuffer buffer = new StringBuffer(i * 4);
    for(int j=0; j<i ; j++) {
      buffer.append("h").append(j).append(" ");
    }
    props.put("hosts", buffer.toString());
    props.put("max-attempts", Integer.toString(MAX_RECONNECTS * agents.size()));

    props.put("request-timeout", Integer.toString(MINIMUM_TIMEOUT));
    props.put("connect-timeout", Integer.toString(MINIMUM_TIMEOUT));

    System.out.println(props);

    return props;
  }

  @Override
  public void stop() {
    try {
      client.close();
    } catch (FlumeException fe) {
      addWarn(fe.getMessage(), fe);
    }
  }

  @Override
  protected void append(ILoggingEvent eventObject) {
    String body = layout != null ? layout.doLayout(eventObject) : eventObject.getFormattedMessage();
    Map<String, String> headers = extractHeaders(eventObject);

    Event event = EventBuilder.withBody(body.trim(), UTF_8, headers);
    try {
      client.append(event);
    } catch (EventDeliveryException ede) {
      addError(ede.getMessage(), ede);
    }
  }

  private Map<String, String> extractHeaders(ILoggingEvent eventObject) {
    Map<String, String> headers = new HashMap<String, String>(10);
    headers.put("timestamp", Long.toString(eventObject.getTimeStamp()));
    headers.put("type", eventObject.getLevel().toString());
    headers.put("logger", eventObject.getLoggerName());
    headers.put("message", eventObject.getMessage());
    try {
      headers.put("host", resolveHostname());
    } catch (UnknownHostException e) {
      addWarn(e.getMessage());
    }
    headers.put("thread", eventObject.getThreadName());
    if (StringUtils.isNotEmpty(application)) {
      headers.put("application", application);
    }

    if(StringUtils.isNotEmpty(type)) {
      headers.put("type", type);
    }

    return headers;
  }

  private String resolveHostname() throws UnknownHostException {
    return hostname != null ? hostname : InetAddress.getLocalHost().getHostName();
  }

  private String resolveApplication() {
    return System.getProperty("application.name");
  }
}
