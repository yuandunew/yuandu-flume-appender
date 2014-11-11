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
import java.util.HashMap;
import java.util.Map;

public class FlumeLogstashV1Appender extends UnsynchronizedAppenderBase<ILoggingEvent> {

  protected static final Charset UTF_8 = Charset.forName("UTF-8");

  private static final String DEFAULT_PATTERN = "%msg%n%ex{full}";

  private RpcClient client;

  private String flumeHostname;

  private int flumePort;

  private String application;

  private String hostname;

  private String type;

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getApplication() {
    return application;
  }

  public void setApplication(String application) {
    this.application = application;
  }

  protected Layout<ILoggingEvent> layout;

  public String getFlumeHostname() {
    return flumeHostname;
  }

  public void setFlumeHostname(String flumeHostname) {
    this.flumeHostname = flumeHostname;
  }

  public int getFlumePort() {
    return flumePort;
  }

  public void setFlumePort(int flumePort) {
    this.flumePort = flumePort;
  }

  public Layout<ILoggingEvent> getLayout() {
    return layout;
  }

  public void setLayout(Layout<ILoggingEvent> layout) {
    this.layout = layout;
  }

  @Override
  public void start() {
    if (layout == null) {
      addWarn("Layout was not defined, will only log the message, no stack traces or custom layout");
    }
    if(StringUtils.isEmpty(application)) {
      application = resolveApplication();
    }

    client = RpcClientFactory.getDefaultInstance(flumeHostname, flumePort);

    super.start();

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
