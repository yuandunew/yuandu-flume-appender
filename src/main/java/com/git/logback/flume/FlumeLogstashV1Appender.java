package com.git.logback.flume;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlumeLogstashV1Appender extends UnsynchronizedAppenderBase<ILoggingEvent> {

  protected static final Charset UTF_8 = Charset.forName("UTF-8");

  private FlumeAvroManager flumeManager;

  private String flumeAgents;

  private String application;

  protected Layout<ILoggingEvent> layout;

  private String hostname;

  private String type;

  public void setType(String type) {
    this.type = type;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public void setApplication(String application) {
    this.application = application;
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
    if (StringUtils.isEmpty(application)) {
      application = resolveApplication();
    }

    if (StringUtils.isNotEmpty(flumeAgents)) {
      String[] agentConfigs = flumeAgents.split(",");

      List<RemoteFlumeAgent> agents = new ArrayList<RemoteFlumeAgent>(agentConfigs.length);
      for (String conf : agentConfigs) {
        RemoteFlumeAgent agent = RemoteFlumeAgent.fromString(conf.trim());
        if (agent != null) {
          agents.add(agent);
        } else {
          addWarn("Cannot build a Flume agent config for '" + conf + "'");
        }
      }
      flumeManager = FlumeAvroManager.create(agents, this);
    } else {
      addError("Cannot configure a flume agent with an empty configuration");
    }
    super.start();

  }

  @Override
  public void stop() {
    try {
      if (flumeManager != null) {
        flumeManager.stop();
      }
    } catch (FlumeException fe) {
      addWarn(fe.getMessage(), fe);
    }
  }

  @Override
  protected void append(ILoggingEvent eventObject) {

    if (flumeManager != null) {
      try {
        String body = layout != null ? layout.doLayout(eventObject) : eventObject.getFormattedMessage();
        Map<String, String> headers = extractHeaders(eventObject);

        Event event = EventBuilder.withBody(body.trim(), UTF_8, headers);

        flumeManager.send(event);
      } catch( Exception e) {
        addError(e.getLocalizedMessage(), e);
      }
    }

  }

  private Map<String, String> extractHeaders(ILoggingEvent eventObject) {
    Map<String, String> headers = new HashMap<String, String>(10);
    headers.put("timestamp", Long.toString(eventObject.getTimeStamp()));
    headers.put("type", eventObject.getLevel().toString());
    headers.put("logger", eventObject.getLoggerName());
    headers.put("message", eventObject.getMessage());
    headers.put("level", eventObject.getLevel().toString());
    try {
      headers.put("host", resolveHostname());
    } catch (UnknownHostException e) {
      addWarn(e.getMessage());
    }
    headers.put("thread", eventObject.getThreadName());
    if (StringUtils.isNotEmpty(application)) {
      headers.put("application", application);
    }

    if (StringUtils.isNotEmpty(type)) {
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
