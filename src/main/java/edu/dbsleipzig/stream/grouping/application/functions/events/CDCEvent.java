package edu.dbsleipzig.stream.grouping.application.functions.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;


public class CDCEvent implements Serializable {
  private String id;
  private long txId;
  private int seq;
  private Metadata metadata;
  private Event event;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public long getTxId() {
    return txId;
  }

  public void setTxId(long txId) {
    this.txId = txId;
  }

  public int getSeq() {
    return seq;
  }

  public void setSeq(int seq) {
    this.seq = seq;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public Event getEvent() {
    return event;
  }

  public void setEvent(Event event) {
    this.event = event;
  }

  public static class Metadata implements Serializable {
    private String authenticatedUser;
    private String executingUser;
    private String connectionType;
    private String connectionClient;
    private String connectionServer;
    private String serverId;
    private String captureMode;
    private DateTime txStartTime;
    private DateTime txCommitTime;
    private Map<String, Object> txMetadata;

    public String getAuthenticatedUser() {
      return authenticatedUser;
    }

    public void setAuthenticatedUser(String authenticatedUser) {
      this.authenticatedUser = authenticatedUser;
    }

    public String getExecutingUser() {
      return executingUser;
    }

    public void setExecutingUser(String executingUser) {
      this.executingUser = executingUser;
    }

    public String getConnectionType() {
      return connectionType;
    }

    public void setConnectionType(String connectionType) {
      this.connectionType = connectionType;
    }

    public String getConnectionClient() {
      return connectionClient;
    }

    public void setConnectionClient(String connectionClient) {
      this.connectionClient = connectionClient;
    }

    public String getConnectionServer() {
      return connectionServer;
    }

    public void setConnectionServer(String connectionServer) {
      this.connectionServer = connectionServer;
    }

    public String getServerId() {
      return serverId;
    }

    public void setServerId(String serverId) {
      this.serverId = serverId;
    }

    public String getCaptureMode() {
      return captureMode;
    }

    public void setCaptureMode(String captureMode) {
      this.captureMode = captureMode;
    }

    public DateTime getTxStartTime() {
      return txStartTime;
    }

    public void setTxStartTime(DateTime txStartTime) {
      this.txStartTime = txStartTime;
    }

    public DateTime getTxCommitTime() {
      return txCommitTime;
    }

    public void setTxCommitTime(DateTime txCommitTime) {
      this.txCommitTime = txCommitTime;
    }

    public Map<String, Object> getTxMetadata() {
      return txMetadata;
    }

    public void setTxMetadata(Map<String, Object> txMetadata) {
      this.txMetadata = txMetadata;
    }
  }

  public static class Event implements Serializable {
    private String elementId;
    private String eventType;
    private String operation;
    private Trip start;
    private Trip end;
    private Object[] keys;
    private State state;
    private String type;

    public String getElementId() {
      return elementId;
    }

    public void setElementId(String elementId) {
      this.elementId = elementId;
    }

    public String getEventType() {
      return eventType;
    }

    public void setEventType(String eventType) {
      this.eventType = eventType;
    }

    public String getOperation() {
      return operation;
    }

    public void setOperation(String operation) {
      this.operation = operation;
    }

    public Trip getStart() {
      return start;
    }

    public void setStart(Trip start) {
      this.start = start;
    }

    public Trip getEnd() {
      return end;
    }

    public void setEnd(Trip end) {
      this.end = end;
    }

    public Object[] getKeys() {
      return keys;
    }

    public void setKeys(Object[] keys) {
      this.keys = keys;
    }

    public State getState() {
      return state;
    }

    public void setState(State state) {
      this.state = state;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }
  }

  public static class Trip implements Serializable {
    private String elementId;
    private String[] labels;
    private Map<String, Object> keys;

    public String getElementId() {
      return elementId;
    }

    public void setElementId(String elementId) {
      this.elementId = elementId;
    }

    public String[] getLabels() {
      return labels;
    }

    public void setLabels(String[] labels) {
      this.labels = labels;
    }

    public Map<String, Object> getKeys() {
      return keys;
    }

    public void setKeys(Map<String, Object> keys) {
      this.keys = keys;
    }
  }

  public static class State implements Serializable {
    private Object before;
    private After after;

    public Object getBefore() {
      return before;
    }

    public void setBefore(Object before) {
      this.before = before;
    }

    public After getAfter() {
      return after;
    }

    public void setAfter(After after) {
      this.after = after;
    }
  }


  public static class After implements Serializable {
    private Property properties;

    public Property getProperties() {
      return properties;
    }

    public void setProperties(Property properties) {
      this.properties = properties;
    }
  }

  public static class Property implements Serializable {
    private DateTime startTime;
    private DateTime tripId;
    private DateTime rideType;
    private DateTime endTime;
    private DateTime userType;
    private DateTime duration;

    public DateTime getStartTime() {
      return startTime;
    }

    public void setStartTime(DateTime startTime) {
      this.startTime = startTime;
    }

    public DateTime getTripId() {
      return tripId;
    }

    public void setTripId(DateTime tripId) {
      this.tripId = tripId;
    }

    public DateTime getRideType() {
      return rideType;
    }

    public void setRideType(DateTime rideType) {
      this.rideType = rideType;
    }

    public DateTime getEndTime() {
      return endTime;
    }

    public void setEndTime(DateTime endTime) {
      this.endTime = endTime;
    }

    public DateTime getUserType() {
      return userType;
    }

    public void setUserType(DateTime userType) {
      this.userType = userType;
    }

    public DateTime getDuration() {
      return duration;
    }

    public void setDuration(DateTime duration) {
      this.duration = duration;
    }
  }

  public static class DateTime implements Serializable {
    private Object B;
    private Object I64;
    private Object F64;
    private String S;
    private Object BA;
    private Object TLD;
    private Object TLDT;
    private Object TLT;
    private String TZDT;
    private Object TOT;
    private Object TD;
    private Object SP;
    private Object LB;
    private Object LI64;
    private Object LF64;
    private Object LS;
    private Object LTLD;
    private Object LTLDT;
    private Object LTLT;
    private Object LZDT;
    private Object LTOT;
    private Object LTD;
    private Object LSP;

    @JsonProperty("B")
    public Object getB() {
      return B;
    }

    @JsonProperty("B")
    public void setB(Object b) {
      B = b;
    }

    @JsonProperty("I64")
    public Object getI64() {
      return I64;
    }

    @JsonProperty("I64")
    public void setI64(Object i64) {
      I64 = i64;
    }

    @JsonProperty("F64")
    public Object getF64() {
      return F64;
    }

    @JsonProperty("F64")
    public void setF64(Object f64) {
      F64 = f64;
    }

    @JsonProperty("S")
    public String getS() {
      return S;
    }

    @JsonProperty("S")
    public void setS(String s) {
      S = s;
    }

    @JsonProperty("BA")
    public Object getBA() {
      return BA;
    }

    @JsonProperty("BA")
    public void setBA(Object BA) {
      this.BA = BA;
    }

    @JsonProperty("TLD")
    public Object getTLD() {
      return TLD;
    }

    @JsonProperty("TLD")
    public void setTLD(Object TLD) {
      this.TLD = TLD;
    }

    @JsonProperty("TLDT")
    public Object getTLDT() {
      return TLDT;
    }

    @JsonProperty("TLDT")
    public void setTLDT(Object TLDT) {
      this.TLDT = TLDT;
    }

    @JsonProperty("TLT")
    public Object getTLT() {
      return TLT;
    }

    @JsonProperty("TLT")
    public void setTLT(Object TLT) {
      this.TLT = TLT;
    }

    @JsonProperty("TZDT")
    public String getTZDT() {
      return TZDT;
    }

    @JsonProperty("TZDT")
    public void setTZDT(String TZDT) {
      this.TZDT = TZDT;
    }

    @JsonProperty("TOT")
    public Object getTOT() {
      return TOT;
    }

    @JsonProperty("TOT")
    public void setTOT(Object TOT) {
      this.TOT = TOT;
    }

    @JsonProperty("TD")
    public Object getTD() {
      return TD;
    }

    @JsonProperty("TD")
    public void setTD(Object TD) {
      this.TD = TD;
    }

    @JsonProperty("SP")
    public Object getSP() {
      return SP;
    }

    @JsonProperty("SP")
    public void setSP(Object SP) {
      this.SP = SP;
    }

    @JsonProperty("LB")
    public Object getLB() {
      return LB;
    }

    @JsonProperty("LB")
    public void setLB(Object LB) {
      this.LB = LB;
    }

    @JsonProperty("LI64")
    public Object getLI64() {
      return LI64;
    }

    @JsonProperty("LI64")
    public void setLI64(Object LI64) {
      this.LI64 = LI64;
    }

    @JsonProperty("LF64")
    public Object getLF64() {
      return LF64;
    }

    @JsonProperty("LF64")
    public void setLF64(Object LF64) {
      this.LF64 = LF64;
    }

    @JsonProperty("LS")
    public Object getLS() {
      return LS;
    }

    @JsonProperty("LS")
    public void setLS(Object LS) {
      this.LS = LS;
    }

    @JsonProperty("LTLD")
    public Object getLTLD() {
      return LTLD;
    }

    @JsonProperty("LTLD")
    public void setLTLD(Object LTLD) {
      this.LTLD = LTLD;
    }

    @JsonProperty("LTLDT")
    public Object getLTLDT() {
      return LTLDT;
    }

    @JsonProperty("LTLDT")
    public void setLTLDT(Object LTLDT) {
      this.LTLDT = LTLDT;
    }

    @JsonProperty("LTLT")
    public Object getLTLT() {
      return LTLT;
    }

    @JsonProperty("LTLT")
    public void setLTLT(Object LTLT) {
      this.LTLT = LTLT;
    }

    @JsonProperty("LZDT")
    public Object getLZDT() {
      return LZDT;
    }

    @JsonProperty("LZDT")
    public void setLZDT(Object LZDT) {
      this.LZDT = LZDT;
    }

    @JsonProperty("LTOT")
    public Object getLTOT() {
      return LTOT;
    }

    @JsonProperty("LTOT")
    public void setLTOT(Object LTOT) {
      this.LTOT = LTOT;
    }

    @JsonProperty("LTD")
    public Object getLTD() {
      return LTD;
    }

    @JsonProperty("LTD")
    public void setLTD(Object LTD) {
      this.LTD = LTD;
    }

    @JsonProperty("LSP")
    public Object getLSP() {
      return LSP;
    }

    @JsonProperty("LSP")
    public void setLSP(Object LSP) {
      this.LSP = LSP;
    }
  }
}
