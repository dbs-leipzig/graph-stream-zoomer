package edu.dbsleipzig.stream.grouping.application.functions;

import edu.dbsleipzig.stream.grouping.application.functions.events.CDCEvent;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.time.Instant;


public class CDCEventToTripleMapper implements FlatMapFunction<CDCEvent, StreamTriple> {
  public CDCEventToTripleMapper() {

  }

  private static final long serialVersionUID = 1L;


  @Override
  public void flatMap(CDCEvent event, Collector<StreamTriple> out) throws Exception {

    if (event == null) {
      return;
    }

    // Parse the String to an Instant
    Instant instant = Instant.parse(event.getMetadata().getTxStartTime().getTZDT());

    StreamTriple triple = new StreamTriple();

    triple.setId(event.getId());
    triple.setLabel(event.getEvent().getType());
    triple.setTimestamp(Timestamp.from(instant));

    Properties properties = new Properties();
    properties.set("operation", event.getEvent().getOperation());
    properties.set("startTime", event.getEvent().getState().getAfter().getProperties().getStartTime().getTZDT());
    properties.set("tripId",event.getEvent().getState().getAfter().getProperties().getTripId().getS());
    properties.set("rideType",event.getEvent().getState().getAfter().getProperties().getRideType().getS());
    properties.set("endTime", event.getEvent().getState().getAfter().getProperties().getEndTime().getTZDT());
    properties.set("userType", event.getEvent().getState().getAfter().getProperties().getUserType().getS());
    properties.set("duration", event.getEvent().getState().getAfter().getProperties().getDuration().getI64());
    triple.setProperties(properties);

    StreamVertex src = new StreamVertex();
    String srcId = event.getEvent().getStart().getElementId();
    src.setVertexLabel(event.getEvent().getStart().getLabels()[0]);
    src.setVertexId(srcId);
    src.setEventTime(Timestamp.from(instant));
    Properties srcProperties = new Properties();
    srcProperties.set("id", srcId);
    src.setVertexProperties(srcProperties);

    triple.setSource(src);

    StreamVertex trg = new StreamVertex();
    String trgId = event.getEvent().getEnd().getElementId();
    trg.setVertexLabel(event.getEvent().getEnd().getLabels()[0]);
    trg.setVertexId(trgId);
    trg.setEventTime(Timestamp.from(instant));
    Properties trgProperties = new Properties();
    trgProperties.set("id", trgId);
    trg.setVertexProperties(trgProperties);

    triple.setTarget(trg);

    out.collect(triple);
  }
}
