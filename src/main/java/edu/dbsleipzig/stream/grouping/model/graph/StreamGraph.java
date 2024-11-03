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
package edu.dbsleipzig.stream.grouping.model.graph;

import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.Extractor;
import edu.dbsleipzig.stream.grouping.model.graph.functions.TripleRowToStreamTripleMap;
import edu.dbsleipzig.stream.grouping.model.table.TableSet;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;

import static edu.dbsleipzig.stream.grouping.model.table.TableSet.*;
import static edu.dbsleipzig.stream.grouping.model.table.TableSet.FIELD_EDGE_ID;
import static org.apache.flink.table.api.Expressions.$;

/**
 * stream graph class one of the base concepts of the stream of objects as edge stream.
 * <p>
 * Furthermore, a stream graph provides operations that are performed on the underlying data.
 * These operations result in another stream graph.
 * <p>
 * this implementation is based on dynamic Tables instead of Data streams .
 * <p>
 * A stream graph is wrapping a {@link StreamGraphLayout} which defines, how the graph is
 * represented in Apache Flink Table API.
 */

public class StreamGraph extends StreamGraphLayout {

    /**
     * Creates a new stream graph based on a vertex and edge stream.
     *
     * @param vertices the vertex stream
     * @param edges  the edge stream
     * @param config the the stream graph configuration
     */
    public StreamGraph(DataStream<StreamVertex> vertices, DataStream<StreamEdge> edges,
      StreamGraphConfig config) {
        super(Objects.requireNonNull(vertices), Objects.requireNonNull(edges),
          Objects.requireNonNull(config));
    }

    /**
     * Creates a new stream graph based on a tableset.
     *
     * @param tableSet representation of the stream graph as tables
     * @param config the stream graph configuration
     */
    public StreamGraph(TableSet tableSet, StreamGraphConfig config) {
        super(Objects.requireNonNull(tableSet), Objects.requireNonNull(config));
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
     */
    public StreamGraph groupBy(List<String> vertexGroupingKeys,
                               List<CustomizedAggregationFunction> vertexAggregateFunctions,
                               List<String> edgeGroupingKeys,
                               List<CustomizedAggregationFunction> edgeAggregateFunctions) {
        return (StreamGraph) super.groupBy(vertexGroupingKeys, vertexAggregateFunctions, edgeGroupingKeys, edgeAggregateFunctions);
    }

    /**
     * Api function for applying a graph stream operator to a stream graph instance.
     *
     * @param operator the operator to apply
     * @return the resulting graph stream
     */
    public StreamGraph apply(GraphStreamToGraphStreamOperator operator) {
        return operator.execute(this);
    }


    public DataStream<StreamTriple> toTripleStream() {
        Table joinedTableEdgesVertices = createStreamTriple(getTableSet().getVertices(),
          getTableSet().getEdges());

        DataStream<Row> rowStreamEdgesVertices =
          getConfig().getTableEnvironment().toDataStream(joinedTableEdgesVertices);

        return rowStreamEdgesVertices
          .map(new TripleRowToStreamTripleMap());
    }

    public void print() {
        toTripleStream().print();
    }

    /**
     * Prints the vertices of this {@link StreamGraph} instance on stdout as table.
     */
    public void printVertices() {
        getVertexStream().print();
    }

    /**
     * Prints the edges of this {@link StreamGraph} instance on stdout as table.
     */
    public void printEdges() {
        getEdgeStream().print();
    }

    /**
     * Writes the resulting super edges and vertices.
     */
    public void writeAsCsv(String path) {
        final StreamingFileSink<StreamVertex> vertexSink =
          StreamingFileSink.forRowFormat(new Path(path + "_V"),
              new SimpleStringEncoder<StreamVertex>("UTF-8"))
            .withBucketAssigner(new BasePathBucketAssigner<>())
            .build();

        final StreamingFileSink<StreamEdge> edgeSink =
          StreamingFileSink.forRowFormat(new Path(path + "_E"),
              new SimpleStringEncoder<StreamEdge>("UTF-8"))
            .withBucketAssigner(new BasePathBucketAssigner<>())
            .build();

        getVertexStream().addSink(vertexSink);
        getEdgeStream().addSink(edgeSink);
    }

    /**
     * Returns joined table for edges and vertices.
     *
     * @param vertices Grouped vertices-table of the StreamGraph
     * @param edges Grouped edges-table of the StreamGraph
     * @return twice-joined table for edges, source- and target-vertices
     */
    public Table createStreamTriple(Table vertices, Table edges) {
        String edgeEventTime = getConfig().createUniqueAttributeName();
        String sourceVertexEventTime = getConfig().createUniqueAttributeName();
        String targetVertexEventTime = getConfig().createUniqueAttributeName();

        // Join source-vertices and edges based on IDs and window-time
        Table joinedEdgesWithSourceVertices =
          vertices.select(
            $(FIELD_VERTEX_ID),
            $(FIELD_EVENT_TIME).cast(DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class)).as(sourceVertexEventTime),
            $(FIELD_VERTEX_LABEL).as(FIELD_VERTEX_SOURCE_LABEL),
            $(FIELD_VERTEX_PROPERTIES).as(FIELD_VERTEX_SOURCE_PROPERTIES))
            .join(
              edges.select(
                $(FIELD_EDGE_ID),
                $(FIELD_EVENT_TIME).as(edgeEventTime),
                $(FIELD_SOURCE_ID),
                $(FIELD_TARGET_ID),
                $(FIELD_EDGE_LABEL),
                $(FIELD_EDGE_PROPERTIES)))
            .where(
              $(FIELD_VERTEX_ID).isEqual($(FIELD_SOURCE_ID))
                .and($(sourceVertexEventTime).isEqual($(edgeEventTime))))
            .select(
              $(FIELD_SOURCE_ID),
              $(FIELD_VERTEX_SOURCE_LABEL),
              $(FIELD_VERTEX_SOURCE_PROPERTIES),
              $(FIELD_EDGE_ID),
              $(edgeEventTime),
              $(FIELD_EDGE_LABEL),
              $(FIELD_EDGE_PROPERTIES),
              $(FIELD_TARGET_ID));

        // Second join to join the edges with the target-vertices based on IDs and window-time
        return joinedEdgesWithSourceVertices
          .join(
            vertices.select(
              $(FIELD_VERTEX_ID),
              $(FIELD_EVENT_TIME).cast(DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class)).as(targetVertexEventTime),
              $(FIELD_VERTEX_LABEL).as(FIELD_VERTEX_TARGET_LABEL),
              $(FIELD_VERTEX_PROPERTIES).as(FIELD_VERTEX_TARGET_PROPERTIES)))
          .where(
            $(FIELD_TARGET_ID).isEqual($(FIELD_VERTEX_ID))
              .and($(edgeEventTime).isLessOrEqual($(targetVertexEventTime)))
                        .and(($(edgeEventTime).isGreaterOrEqual($(targetVertexEventTime)))))
          .select(
            $(FIELD_SOURCE_ID),
            $(FIELD_VERTEX_SOURCE_LABEL),
            $(FIELD_VERTEX_SOURCE_PROPERTIES),
            $(FIELD_EDGE_ID),
            $(edgeEventTime).as(FIELD_EVENT_TIME),
            $(FIELD_EDGE_LABEL),
            $(FIELD_EDGE_PROPERTIES),
            $(FIELD_TARGET_ID),
            $(FIELD_VERTEX_TARGET_LABEL),
            $(FIELD_VERTEX_TARGET_PROPERTIES));
    }

    private DataStream<StreamEdge> getEdgeStream() {
        return getConfig().getTableEnvironment()
          .toDataStream(getTableSet().getEdges(), StreamEdge.class);
    }

    private DataStream<StreamVertex> getVertexStream() {
        return getConfig().getTableEnvironment()
          .toDataStream(getTableSet().getVertices(), StreamVertex.class);
    }

    /*
     * STATIC FUNCTIONS
     */

    /**
     * Creates a StreamGraph instance from a triple stream. The triple will be split in vertices and edges.
     *
     * @param stream the triple stream
     * @param config the config
     * @return a StreamGraph instance
     */
    public static StreamGraph fromFlinkStream(DataStream<StreamTriple> stream, StreamGraphConfig config) {
        stream = stream.assignTimestampsAndWatermarks(
          WatermarkStrategy
            .<StreamTriple>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime()));
        SingleOutputStreamOperator<StreamEdge> edges = stream.process(new Extractor());
        DataStream<StreamVertex> vertices = edges.getSideOutput(Extractor.VERTEX_OUTPUT_TAG);
        return new StreamGraph(vertices, edges, config);
    }
}
