package org.springframework.data.document.analytics;

import java.util.Date;

public class MvcEvent {

  private String controller;

  private String action;

  private Parameters parameters;

  private Date date;

  private String requestUri;

  private String requestAddress;

  private String remoteUser;

  private String view;

  public String getController() {
    return controller;
  }

  public void setController(String controller) {
    this.controller = controller;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public Parameters getParameters() {
    return parameters;
  }

  public void setParameters(Parameters parameters) {
    this.parameters = parameters;
  }

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public String getRequestUri() {
    return requestUri;
  }

  public void setRequestUri(String requestUri) {
    this.requestUri = requestUri;
  }

  public String getRequestAddress() {
    return requestAddress;
  }

  public void setRequestAddress(String requestAddress) {
    this.requestAddress = requestAddress;
  }

  public String getRemoteUser() {
    return remoteUser;
  }

  public void setRemoteUser(String remoteUser) {
    this.remoteUser = remoteUser;
  }

  //TODO
  //Map sessionAttributes


}
