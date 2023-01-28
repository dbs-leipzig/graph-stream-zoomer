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
package edu.dbsleipzig.stream.grouping.model.graph;

import edu.dbsleipzig.stream.grouping.impl.algorithm.GraphStreamGrouping;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.dbsleipzig.stream.grouping.model.table.TableSet;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;
import java.util.Objects;

/**
 * A stream graph layout is wrapping a {@link TableSet} which defines, how the layout is
 * represented in Apache Flink table API.
 * here we three tables (edges, vertices , graph)
 */
public class StreamGraphLayout {
  /**
   * Stream graph Configuration
   */
  private final StreamGraphConfig config;

  /**
   * Table set the layout is based on
   */
  private final TableSet tableSet;

  /**
   * Constructor used for input data stream.
   *
   * @param vertices stream of vertices
   * @param edges stream of edges
   * @param config the graph stream configuration
   */
  public StreamGraphLayout(DataStream<StreamVertex> vertices, DataStream<StreamEdge> edges,
    StreamGraphConfig config) {

    TableSet tableSet = new TableSet();

    tableSet.put(TableSet.TABLE_VERTICES,
      config.getTableEnvironment().fromDataStream(vertices, TableSet.getVertexSchema()));

    tableSet.put(TableSet.TABLE_EDGES,
      config.getTableEnvironment().fromDataStream(edges, TableSet.getEdgeSchema()));

    this.tableSet = tableSet;
    this.config = Objects.requireNonNull(config);
  }

  /**
   * Constructor used after grouping is applied
   *
   * @param tableSet table set
   * @param config   graph stream configuration
   */
  public StreamGraphLayout(TableSet tableSet, StreamGraphConfig config) {
    this.tableSet = Objects.requireNonNull(tableSet);
    this.config = Objects.requireNonNull(config);
  }

  /**
   * Returns the stream graph Configuration
   *
   * @return stream graph Configuration
   */
  public StreamGraphConfig getConfig() {
    return config;
  }

  /**
   * Returns the table set .
   *
   * @return table set.
   */
  public TableSet getTableSet() {
    return tableSet;
  }

  /**
   * Creates a condensed version of the stream graph by grouping vertices and edges based on given
   * property keys.
   * <p>
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys. Furthermore, one can
   * specify sets of vertex and edge aggregate functions which are applied on vertices/edges
   * represented by the same super vertex/edge.
   * <p>
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   *
   * @param vertexGroupingKeys       property keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys         property keys to group edges
   * @param edgeAggregateFunctions   aggregate functions to apply on super edges
   * @return summary graph
   * @see GraphStreamGrouping
   */
  StreamGraphLayout groupBy(List<String> vertexGroupingKeys,
                            List<CustomizedAggregationFunction> vertexAggregateFunctions,
                            List<String> edgeGroupingKeys,
                            List<CustomizedAggregationFunction> edgeAggregateFunctions
  ) {
    Objects.requireNonNull(vertexGroupingKeys, "missing vertex grouping key(s)");

    GraphStreamGrouping.GroupingBuilder builder = new GraphStreamGrouping.GroupingBuilder();

    builder.addVertexGroupingKeys(vertexGroupingKeys);

    if (edgeGroupingKeys != null) {
      builder.addEdgeGroupingKeys(edgeGroupingKeys);
    }

    if (vertexAggregateFunctions != null) {
      for (CustomizedAggregationFunction f : vertexAggregateFunctions) {
        builder.addVertexAggregateFunction(f);
      }
    }

    if (edgeAggregateFunctions != null) {
      for (CustomizedAggregationFunction f : edgeAggregateFunctions) {
        builder.addEdgeAggregateFunction(f);
      }
    }

    return builder.build().execute(this);
  }
}
