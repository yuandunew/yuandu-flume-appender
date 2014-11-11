package com.git.logback;

import org.apache.commons.lang.StringUtils;

public final class RemoteFlumeAgent {

  private final String hostname;

  private final int port;

  public RemoteFlumeAgent(final String hostname, final int port) {
    this.hostname = hostname;
    this.port = port;
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  public static RemoteFlumeAgent fromString(String input) {
    if(StringUtils.isNotEmpty(input)) {

      String[] parts = input.split(":");
      if (parts.length == 2) {

        String portString = parts[1];
        try {
          int port = Integer.parseInt(portString);
          return new RemoteFlumeAgent(parts[0], port);
        } catch (NumberFormatException nfe) {
          //TODO Log parsing error
        }
      } else {
        //TODO Log wrong number of segments
      }
    } else {
      //TODO Log empty input
    }
    return null;
  }
}
