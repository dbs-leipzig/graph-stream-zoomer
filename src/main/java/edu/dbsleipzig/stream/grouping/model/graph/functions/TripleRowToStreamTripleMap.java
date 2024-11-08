/*
 * Copyright © 2021 - 2024 Leipzig University (Database Research Group)
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
package edu.dbsleipzig.stream.grouping.model.graph.functions;

import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;

/**
 * This map function maps a {@link Row} back to a {@link StreamTriple} for printing.
 */
public class TripleRowToStreamTripleMap implements MapFunction<Row, StreamTriple> {
  @Override
  public StreamTriple map(Row row) throws Exception {
    String sourceId = row.getFieldAs(0);
    String sourceLabel = row.getFieldAs(1);
    Properties sourceProps = row.getFieldAs(2);
    String edgeId = row.getFieldAs(3);
    Timestamp eventTime =  row.getFieldAs(4);
    String edgeLabel = row.getFieldAs(5);
    Properties edgeProps = row.getFieldAs(6);
    String targetId = row.getFieldAs(7);
    String targetLabel = row.getFieldAs(8);
    Properties targetProps = row.getFieldAs(9);
    StreamVertex sourceVertex = new StreamVertex(sourceId, sourceLabel, sourceProps, eventTime);
    StreamVertex targetVertex = new StreamVertex(targetId, targetLabel, targetProps, eventTime);
    return new StreamTriple(edgeId, eventTime, edgeLabel, edgeProps, sourceVertex, targetVertex);
  }
}
