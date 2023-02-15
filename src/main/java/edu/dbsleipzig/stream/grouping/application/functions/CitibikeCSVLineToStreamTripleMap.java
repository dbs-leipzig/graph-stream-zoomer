/*
 * Copyright © 2021 - 2023 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.dbsleipzig.stream.grouping.application.functions;

import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple15;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Maps a tuple parsed from csv line of the citibike csv file to a StreamTriple.
 */
public class CitibikeCSVLineToStreamTripleMap implements MapFunction<CitibikeTuple15, StreamTriple> {

  @Override
  public StreamTriple map(CitibikeTuple15 tuple15) throws
    Exception {
    StreamTriple streamTriple = new StreamTriple();
    StreamVertex sourceVertex = new StreamVertex();
    StreamVertex targetVertex = new StreamVertex();

    HashMap<String, Object> edgePropMap = new HashMap<>();
    HashMap<String, Object> sourcePropMap = new HashMap<>();
    HashMap<String, Object> targetPropMap = new HashMap<>();

    edgePropMap.put("tripduration", Integer.parseInt(tuple15.f0));
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    Date parseStopTime = simpleDateFormat.parse(tuple15.f2.replaceAll("\"", ""));
    Timestamp stopTime = new Timestamp(parseStopTime.getTime());
    edgePropMap.put("starttime", tuple15.f1);
    edgePropMap.put("stoptime", tuple15.f2);
    streamTriple.setTimestamp(stopTime);
    sourceVertex.setEventTime(stopTime);
    targetVertex.setEventTime(stopTime);
    sourceVertex.setVertexId(tuple15.f3);
    sourceVertex.setVertexLabel("Station");
    sourcePropMap.put("name", tuple15.f4);
    sourcePropMap.put("lat", Double.parseDouble(tuple15.f5));
    sourcePropMap.put("long", Double.parseDouble(tuple15.f6));
    targetVertex.setVertexId(tuple15.f7);
    targetVertex.setVertexLabel("Station");
    if (!tuple15.f9.equalsIgnoreCase("NULL")){
      targetPropMap.put("lat", Double.parseDouble(tuple15.f9));
    }
    if (!tuple15.f10.equalsIgnoreCase("NULL")) {
      targetPropMap.put("long", Double.parseDouble(tuple15.f10));
    }
    targetPropMap.put("name", tuple15.f8);
    streamTriple.setId(DigestUtils.md5Hex(tuple15.f11 + tuple15.f1 + tuple15.f2));
    streamTriple.setLabel("trip");
    if (tuple15.f13 != null && !tuple15.f13.equals("NULL")) {
      edgePropMap.put("birth year", Integer.parseInt(tuple15.f13));
    }
    edgePropMap.put("gender", Integer.parseInt(tuple15.f14));
    edgePropMap.put("usertype", tuple15.f12);
    edgePropMap.put("bikeId", tuple15.f11);
    sourceVertex.setVertexProperties(Properties.createFromMap(sourcePropMap));
    targetVertex.setVertexProperties(Properties.createFromMap(targetPropMap));
    streamTriple.setSource(sourceVertex);
    streamTriple.setTarget(targetVertex);
    streamTriple.setProperties(Properties.createFromMap(edgePropMap));

    return streamTriple;
  }
}
