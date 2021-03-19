package edu.leipzig.model.graph;

import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.impl.functions.utils.Extractor;
import edu.leipzig.model.table.TableSet;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Objects;

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
     * Creates a new stream graph based on the given parameters.
     *
     * @param edges  representation of the stream graph
     * @param config the the stream graph configuration
     */
    public StreamGraph(DataStream<StreamVertex> vertices, DataStream<StreamEdge> edges, StreamGraphConfig config) {
        super(Objects.requireNonNull(vertices), Objects.requireNonNull(edges), config);
    }

    public StreamGraph(TableSet tableSet, StreamGraphConfig config) {
        super(tableSet, config);
    }

    /**
     * Creates a condensed version of the stream graph by grouping vertices based on the specified
     * property keys.
     * <p>
     * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
     * incident vertices.
     *
     * @param vertexGroupingKeys property keys to group vertices
     * @return summary graph
     */
    public StreamGraph groupBy(List<String> vertexGroupingKeys) {
        return (StreamGraph) super.groupBy(vertexGroupingKeys);
    }

    /**
     * Creates a condensed version of the stream graph by grouping vertices and edges based on given
     * property keys.
     * <p>
     * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
     * incident vertices and explicitly by the specified edge grouping keys.
     * <p>
     * One needs to at least specify a list of vertex grouping keys. Any other argument may be
     * {@code null}.
     *
     * @param vertexGroupingKeys property keys to group vertices
     * @param edgeGroupingKeys   property keys to group edges
     * @return summary graph
     */
    public StreamGraph groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
        return (StreamGraph) super.groupBy(vertexGroupingKeys, edgeGroupingKeys);
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
                               List<String> edgeGroupingKeys, List<CustomizedAggregationFunction> edgeAggregateFunctions) {
        return (StreamGraph) super.groupBy(vertexGroupingKeys, vertexAggregateFunctions, edgeGroupingKeys, edgeAggregateFunctions);
    }

    public StreamGraph apply(GraphStreamToGraphStreamOperator operator) {
        return operator.execute(this);
    }

    /**
     * prints the resulting summary graph from its super edges and vertices.
     */
    public void printRichEdges() {
        TableSet tableSet = getConfig().getTableSetFactory().fromTable(
          computeSummarizedGraphTable(
            getTableSet().getEdges(),
            getTableSet().getVertices()),
          getConfig().getTableEnvironment());
        getConfig().getTableEnvironment().toRetractStream(tableSet.getGraph(), Row.class).print();
    }

    /**
     * prints the resulting super edges and vertices.
     */
    public void print() {
        getConfig().getTableEnvironment().toRetractStream(getTableSet().getVertices(), Row.class)
          .print();
        getConfig().getTableEnvironment().toRetractStream(getTableSet().getEdges(), Row.class)
          .print();
    }

    /**
     * writes the resulting super edges and vertices.
     */
    public void writeAsCsv(String path) {

        final StreamingFileSink<Tuple2<Boolean, Row>> vertexSink =
          StreamingFileSink.forRowFormat(new Path(path + "_V"), new SimpleStringEncoder<Tuple2<Boolean, Row>>("UTF-8"))
            .build();

        final StreamingFileSink<Tuple2<Boolean, Row>> edgeSink =
          StreamingFileSink.forRowFormat(new Path(path + "_E"), new SimpleStringEncoder<Tuple2<Boolean, Row>>("UTF-8"))
            .build();

        getConfig().getTableEnvironment().toRetractStream(getTableSet().getVertices(), Row.class)
          .addSink(vertexSink);
        getConfig().getTableEnvironment().toRetractStream(getTableSet().getEdges(), Row.class)
          .addSink(edgeSink);
    }

    /**
     * writes the resulting summary graph from its super edges and vertices.
     */
    public void writeGraphAsCsv(String path) {
        final StreamingFileSink<Tuple2<Boolean, Row>> graphSink =
          StreamingFileSink.forRowFormat(new Path(path), new SimpleStringEncoder<Tuple2<Boolean, Row>>("UTF-8"))
            .build();

        TableSet tableSet = getConfig().getTableSetFactory().fromTable(
          computeSummarizedGraphTable(getTableSet().getEdges(), getTableSet().getVertices()),
          getConfig().getTableEnvironment());

        getConfig().getTableEnvironment().toRetractStream(tableSet.getGraph(), Row.class).addSink(graphSink);
    }

    public static StreamGraph fromFlinkStream(DataStream<StreamObject> stream, StreamGraphConfig config) {
        SingleOutputStreamOperator<StreamEdge> edges = stream.process(new Extractor());
        DataStream<StreamVertex> vertices = edges.getSideOutput(Extractor.VERTEX_OUTPUT_TAG);
        return new StreamGraph(vertices, edges, config);
    }

}
