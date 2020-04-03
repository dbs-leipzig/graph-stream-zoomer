package edu.leipzig.model.streamGraph;

import edu.leipzig.impl.algorithm.GraphSummarizer;
import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.model.table.TableSet;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
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

public class StreamGraph {

    private final StreamGraphLayout layout;
    /**
     * Stream graph configuration
     */
    private final StreamGraphConfig config;

    /**
     * Creates a new stream graph based on the given parameters.
     *
     * @param layout representation of the stream graph
     * @param config the stream graph configuration
     */
    public StreamGraph(StreamGraphLayout layout, StreamGraphConfig config) {
        Objects.requireNonNull(layout);
        Objects.requireNonNull(config);
        this.layout = layout;
        this.config = config;
    }

    /**
     * Creates a new stream graph based on the given parameters.
     *
     * @param edges  representation of the stream graph
     * @param config the the stream graph configuration
     */
    StreamGraph(DataStream vertices, DataStream edges, StreamGraphConfig config) {
        Objects.requireNonNull(vertices);
        Objects.requireNonNull(edges);
        Objects.requireNonNull(config);
        TableSet tableSet = new TableSet();
        tableSet.put(TableSet.TABLE_VERTICES, config.getTableEnvironment().fromDataStream(vertices));
        tableSet.put(TableSet.TABLE_EDGES, config.getTableEnvironment().fromDataStream(edges));
        this.config = config;
        this.layout = new StreamGraphLayout(tableSet, config);
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
     * @see GraphSummarizer
     */
    public StreamGraph groupBy(List<String> vertexGroupingKeys) {
        return new StreamGraph(this.layout.groupBy(vertexGroupingKeys), config);
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
     * @see GraphSummarizer
     */
    public StreamGraph groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
        return new StreamGraph(this.layout.groupBy(vertexGroupingKeys, edgeGroupingKeys), config);
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
     * @see GraphSummarizer
     */
    public StreamGraph groupBy(List<String> vertexGroupingKeys,
                               List<CustomizedAggregationFunction> vertexAggregateFunctions,
                               List<String> edgeGroupingKeys, List<CustomizedAggregationFunction> edgeAggregateFunctions) {
        return new StreamGraph(this.layout.groupBy(vertexGroupingKeys, vertexAggregateFunctions, edgeGroupingKeys,
                edgeAggregateFunctions), config);
    }

    /**
     * prints the resulting summary graph from its super edges and vertices.
     */
    public void writeGraphTo() {
        TableSet tableSet = config.getTableSetFactory().fromTable(
          layout.computeSummarizedGraphTable(
            layout.getTableSet().getEdges(),
            layout.getTableSet().getVertices()),
          config.getTableEnvironment());
        config.getTableEnvironment().toRetractStream(tableSet.getGraph(), Row.class).print();
    }

    /**
     * writes the resulting summary graph from its super edges and vertices.
     */
    public void writeGraphAsCsv(String path) {
        TableSet tableSet = config.getTableSetFactory().fromTable(
                layout.computeSummarizedGraphTable(layout.getTableSet().getEdges(),
                        layout.getTableSet().getVertices()), config.getTableEnvironment());
        config.getTableEnvironment().toRetractStream(tableSet.getGraph(), Row.class)
                .writeAsCsv(path+ "_G", FileSystem.WriteMode.OVERWRITE);
    }

    /**
     * prints the resulting super edges and vertices.
     */
    public void writeTo() {
        config.getTableEnvironment().toRetractStream(this.layout.getTableSet().getVertices(),
                Row.class).print();
       config.getTableEnvironment().toRetractStream(this.layout.getTableSet().getEdges(),
                Row.class).print();
    }

    /**
     * writes the resulting super edges and vertices.
     */
    public void writeAsCsv(String path) {
        config.getTableEnvironment().toRetractStream(this.layout.getTableSet().getVertices(),
                Row.class).writeAsCsv(path + "_V", FileSystem.WriteMode.OVERWRITE);
        config.getTableEnvironment().toRetractStream(this.layout.getTableSet().getEdges(),
                Row.class).writeAsCsv(path + "_E", FileSystem.WriteMode.OVERWRITE);
    }

}
