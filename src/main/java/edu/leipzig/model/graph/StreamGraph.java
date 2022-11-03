package edu.leipzig.model.graph;

import com.sun.org.apache.xpath.internal.operations.Bool;
import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.impl.functions.utils.Extractor;
import edu.leipzig.model.table.TableSet;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static edu.leipzig.model.table.TableSet.*;
import static edu.leipzig.model.table.TableSet.FIELD_EDGE_ID;
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
     * @param config the the stream graph configuration
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

    /**
     * TODO: All the following print and write functions are not working properly atm.
     */


    /**
     * Prints the vertices and edges of this {@link StreamGraph} instance as triples on stdout.
     */
    public void printTriples() {
        TableSet tableSet = getConfig().getTableSetFactory().fromTable(
          computeSummarizedGraphTable(
            getTableSet().getEdges(),
            getTableSet().getVertices()),
          getConfig().getTableEnvironment());
        getConfig().getTableEnvironment().toRetractStream(tableSet.getGraph(), Row.class).print();
    }

    public void print() {
        //Todo: We have to clarify what should be the result of a 'print' on a two-tabled layout
    }

    /**
     * Prints the vertices of this {@link StreamGraph} instance on stdout as table.
     */
    public void printVertices() {
        Schema vertexSchema = Schema.newBuilder()
          .fromResolvedSchema(getTableSet().getVertices().getResolvedSchema())
          .build();

        getConfig().getTableEnvironment()
          //.toDataStream(getTableSet().getVertices(), StreamVertex.class)
          .toChangelogStream(getTableSet().getVertices(), vertexSchema)
          /*.toDataStream(getTableSet().getVertices(), DataTypes.STRUCTURED(
            StreamVertex.class,
            DataTypes.FIELD("vertex_id", DataTypes.STRING()),
            DataTypes.FIELD("vertex_label", DataTypes.STRING()),
            //DataTypes.FIELD("vertex_properties", DataTypes.RAW())),
            DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3))))*/
          .print();
    }

    /**
     * Prints the edges of this {@link StreamGraph} instance on stdout as table.
     */
    public void printEdges() throws Exception {
        Schema edgeSchema = Schema.newBuilder()
          .fromResolvedSchema(getTableSet().getEdges().getResolvedSchema())
          .build();

        getConfig().getTableEnvironment()
          .toChangelogStream(getTableSet().getEdges(), edgeSchema)
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

    public Table createStreamTriple(Table vertices, Table edges) {
        String edgeEventTime = getConfig().createUniqueAttributeName();
        String sourceVertexEventTime = getConfig().createUniqueAttributeName();
        String targetVertexEventTime = getConfig().createUniqueAttributeName();

        Table joinedEdgesWithSourceVertices =
          vertices.select($(FIELD_VERTEX_ID),
            $(FIELD_EVENT_TIME).cast(DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class)).as(sourceVertexEventTime),
            $(FIELD_VERTEX_LABEL).as(FIELD_VERTEX_SOURCE_LABEL), $(FIELD_VERTEX_PROPERTIES).as(FIELD_VERTEX_SOURCE_PROPERTIES))
            .join(
              edges.select($(FIELD_EDGE_ID),
                $(FIELD_EVENT_TIME).as(edgeEventTime),
                $(FIELD_SOURCE_ID), $(FIELD_TARGET_ID), $(FIELD_EDGE_LABEL), $(FIELD_EDGE_PROPERTIES)))
            .where($(FIELD_VERTEX_ID).isEqual($(FIELD_SOURCE_ID)).and($(sourceVertexEventTime).isEqual($(edgeEventTime))))
            .select($(FIELD_SOURCE_ID), $(FIELD_VERTEX_SOURCE_LABEL), $(FIELD_VERTEX_SOURCE_PROPERTIES),
              $(FIELD_EDGE_ID), $(edgeEventTime), $(FIELD_EDGE_LABEL), $(FIELD_EDGE_PROPERTIES),
              $(FIELD_TARGET_ID));


        Table fullyJoinedEdgesAndVertices = joinedEdgesWithSourceVertices.join(vertices.select($(FIELD_VERTEX_ID),
          $(FIELD_EVENT_TIME).cast(DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class)).as(targetVertexEventTime),
          $(FIELD_VERTEX_LABEL).as(FIELD_VERTEX_TARGET_LABEL),
          $(FIELD_VERTEX_PROPERTIES).as(FIELD_VERTEX_TARGET_PROPERTIES))
        ).where($(FIELD_TARGET_ID).isEqual($(FIELD_VERTEX_ID)).and($(edgeEventTime).isEqual($(targetVertexEventTime))))
          .select($(FIELD_SOURCE_ID), $(FIELD_VERTEX_SOURCE_LABEL), $(FIELD_VERTEX_SOURCE_PROPERTIES),
            $(FIELD_EDGE_ID), $(edgeEventTime).as(FIELD_EVENT_TIME), $(FIELD_EDGE_LABEL),
            $(FIELD_EDGE_PROPERTIES),
            $(FIELD_TARGET_ID), $(FIELD_VERTEX_TARGET_LABEL), $(FIELD_VERTEX_TARGET_PROPERTIES));

        return fullyJoinedEdgesAndVertices;
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
        stream.assignTimestampsAndWatermarks(
          WatermarkStrategy
            .<StreamTriple>forBoundedOutOfOrderness(config.getMaxOutOfOrdernessDuration())
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime()));
        SingleOutputStreamOperator<StreamEdge> edges = stream.process(new Extractor());
        DataStream<StreamVertex> vertices = edges.getSideOutput(Extractor.VERTEX_OUTPUT_TAG);
        return new StreamGraph(vertices, edges, config);
    }
}
