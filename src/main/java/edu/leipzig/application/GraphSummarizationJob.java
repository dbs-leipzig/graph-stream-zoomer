package edu.leipzig.application;

import edu.leipzig.impl.algorithm.TableGroupingBase;
import edu.leipzig.impl.functions.aggregation.Count;
import edu.leipzig.model.graph.StreamEdge;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.graph.StreamTriple;
import edu.leipzig.model.graph.StreamVertex;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.stream.Stream;

public class GraphSummarizationJob {

    public static void main(String[] args) throws Exception {

        // Increase number of network buffers to prevent IOException
        Configuration cfg = new Configuration();
        cfg.setString("taskmanager.memory.network.max", "1gb");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(cfg);

        DataStream<StreamTriple> graphStreamTriples = createStreamTriples(env);

        StreamGraph streamGraph = StreamGraph.fromFlinkStream(graphStreamTriples, new StreamGraphConfig(env));

        TableGroupingBase.GroupingBuilder groupingBuilder = new TableGroupingBase.GroupingBuilder();

        // Group edges and vertices on 'label'-property and count the amount
        groupingBuilder.addVertexGroupingKey(":label");
        groupingBuilder.addEdgeGroupingKey(":label");
        groupingBuilder.addVertexAggregateFunction(new Count());
        groupingBuilder.addEdgeAggregateFunction(new Count());

        streamGraph = groupingBuilder.build().execute(streamGraph);

        DataStream<StreamTriple> tripleDataStream = testExecutionPlanDifference(streamGraph);

        DataStream<StreamTriple> testTriple =
          tripleDataStream.join(graphStreamTriples).where(td -> td.f0).equalTo(gt -> gt.f0)
          .window(TumblingEventTimeWindows.of(Time.seconds(10)))
          .apply(new JoinFunction<StreamTriple, StreamTriple, StreamTriple>() {
              @Override
              public StreamTriple join(StreamTriple streamTriple, StreamTriple streamTriple2) throws
                Exception {
                  return streamTriple;
              }
          });

        DataStream<StreamTriple> testTriple2 = tripleDataStream.coGroup(graphStreamTriples)
          .where(t -> t.f0).equalTo(gt -> gt.f0)
          .window(TumblingEventTimeWindows.of(Time.seconds(10)))
          .apply(new CoGroupFunction<StreamTriple, StreamTriple, StreamTriple>() {
              @Override
              public void coGroup(Iterable<StreamTriple> iterable, Iterable<StreamTriple> iterable1,
                Collector<StreamTriple> collector) throws Exception {
              }
          });

        DataStream<StreamTriple> tripleDataStream1 = tripleDataStream.join(tripleDataStream)
          .where(t -> t.f0).equalTo(t1 -> t1.f0)
          .window(TumblingEventTimeWindows.of(Time.seconds(10)))
          .apply(new JoinFunction<StreamTriple, StreamTriple, StreamTriple>() {
              @Override
              public StreamTriple join(StreamTriple streamTriple, StreamTriple streamTriple2) throws
                Exception {
                  return streamTriple;
              }
          });

        System.out.println("Execution Plan: " + env.getExecutionPlan());

        tripleDataStream.print();

        env.execute();
    }

    /**
     * Extracts edges- and vertices-table from StreamGraph. Joins them and maps joined table onto StreamTriples.
     *
     * @param streamGraph StreamGraph with grouped edges and vertices
     * @return DataStream of StreamTriples for joined edges and vertices.
     */
    public static DataStream<StreamTriple> streamGraphToStreamTriple(StreamGraph streamGraph){
        StreamTableEnvironment tEnv = streamGraph.getConfig().getTableEnvironment();
        Table edges = streamGraph.getTableSet().getEdges();
        Table vertices = streamGraph.getTableSet().getVertices();

        //Join edges and vertices to StreamTripleTable
        Table joinedTableEdgesVertices = streamGraph.createStreamTriple(vertices, edges);

        //Use .sqlQuery to avoid ClassCastException for LegacyTypeInformation -> RawType
        DataStream<Row> rowStreamEdgesVertices = tEnv.toDataStream(tEnv.sqlQuery("SELECT * FROM " + joinedTableEdgesVertices));

        //Map each row onto a StreamTriple
        DataStream<StreamTriple> tripleDataStream = rowStreamEdgesVertices.map(new MapFunction<Row, StreamTriple>() {
            @Override
            public StreamTriple map(Row row) throws Exception {
                String sourceId = row.getFieldAs(0);
                String sourceLabel = row.getFieldAs(1);
                Properties sourceProps = row.getFieldAs(2);
                String edgeId = row.getFieldAs(3);
                Timestamp eventTime = Timestamp.valueOf((LocalDateTime) row.getFieldAs(4));
                String edgeLabel = row.getFieldAs(5);
                Properties edgeProps = row.getFieldAs(6);
                String targetId = row.getFieldAs(7);
                String targetLabel = row.getFieldAs(8);
                Properties targetProps = row.getFieldAs(9);
                StreamVertex sourceVertex = new StreamVertex(sourceId, sourceLabel, sourceProps, eventTime);
                StreamVertex targetVertex = new StreamVertex(targetId, targetLabel, targetProps, eventTime);
                return new StreamTriple(edgeId, eventTime, edgeLabel, edgeProps, sourceVertex, targetVertex);
            }
        });
        return tripleDataStream;
    }

    public static DataStream<StreamTriple> testExecutionPlanDifference(StreamGraph streamGraph) {
        StreamTableEnvironment tEnv = streamGraph.getConfig().getTableEnvironment();
        Table edges = streamGraph.getTableSet().getEdges();
        Table vertices = streamGraph.getTableSet().getVertices();

        DataStream<Tuple2<Boolean, StreamEdge>> edgeDataStream = tEnv.toRetractStream(edges,
          StreamEdge.class);
        DataStream<Tuple2<Boolean, StreamVertex>> vertexDataStream = tEnv.toRetractStream(vertices,
          StreamVertex.class);

        edgeDataStream = edgeDataStream.assignTimestampsAndWatermarks(WatermarkStrategy
          .<Tuple2<Boolean, StreamEdge>>forBoundedOutOfOrderness(streamGraph.getConfig().getMaxOutOfOrdernessDuration())
        .withTimestampAssigner((event, timestamp) -> event.f1.getEventTime().getTime()));

        vertexDataStream = vertexDataStream.assignTimestampsAndWatermarks(WatermarkStrategy
        .<Tuple2<Boolean, StreamVertex>>forBoundedOutOfOrderness(streamGraph.getConfig().getMaxOutOfOrdernessDuration())
        .withTimestampAssigner((event, timestamp) -> event.f1.getEventTime().getTime()));


        DataStream<StreamTriple> firstJoinedStream = edgeDataStream.join(vertexDataStream)
          .where(e -> e.f1.getSourceId()).equalTo(v -> v.f1.getVertexId())
          .window(TumblingEventTimeWindows.of(Time.seconds(10)))
          .apply(new JoinFunction<Tuple2<Boolean, StreamEdge>, Tuple2<Boolean, StreamVertex>, StreamTriple>() {
              @Override
              public StreamTriple join(Tuple2<Boolean, StreamEdge> booleanStreamEdgeTuple2,
                Tuple2<Boolean, StreamVertex> booleanStreamVertexTuple2) throws Exception {
                  StreamEdge streamEdge = booleanStreamEdgeTuple2.f1;
                  StreamVertex streamVertex = booleanStreamVertexTuple2.f1;
                  StreamVertex dummyTargetVertex = new StreamVertex(streamEdge.getTargetId(), null, null,
                    null);
                  return new StreamTriple(streamEdge.getEdgeId(), streamEdge.getEventTime(),
                    streamEdge.getEdgeLabel(), streamEdge.getEdgeProperties(), streamVertex, dummyTargetVertex);
              }
          });

        DataStream<StreamTriple> secondJoinedStream = firstJoinedStream.join(vertexDataStream)
          .where(fj -> fj.f5.getVertexId()).equalTo(v -> v.f1.getVertexId())
          .window(TumblingEventTimeWindows.of(Time.seconds(10)))
          .apply(new JoinFunction<StreamTriple, Tuple2<Boolean, StreamVertex>, StreamTriple>() {
              @Override
              public StreamTriple join(StreamTriple streamTriple,
                Tuple2<Boolean, StreamVertex> booleanStreamVertexTuple2) throws Exception {
                  StreamVertex streamVertex = booleanStreamVertexTuple2.f1;
                  return new StreamTriple(streamTriple.f0, streamTriple.f1, streamTriple.f2,
                    streamTriple.f3, streamTriple.f4, streamVertex);
              }
          });

        return secondJoinedStream;
    }

    /**
     * Creates custom StreamTriples.
     *
     * @param env StreamTableEnvironment of the StreamGraph
     * @return StreamTriples derived from custom edges and vertices.
     */
    public static DataStream<StreamTriple> createStreamTriples(StreamExecutionEnvironment env) {

        // t(i+1) = t(i) + 10s
        Timestamp t1 = new Timestamp(1619511661000L);
        Timestamp t2 = new Timestamp(1619511662000L);
        Timestamp t3 = new Timestamp(1619511673000L);
        Timestamp t4 = new Timestamp(1619511674000L);


        // Create vertices with empty properties
        StreamVertex v1 = new StreamVertex("v1", "A", Properties.create(), t1);
        StreamVertex v2 = new StreamVertex("v2", "B", Properties.create(), t1);
        StreamVertex v3 = new StreamVertex("v3", "A", Properties.create(), t2);
        StreamVertex v4 = new StreamVertex("v4", "B", Properties.create(), t2);
        StreamVertex v5 = new StreamVertex("v5", "A", Properties.create(), t3);
        StreamVertex v6 = new StreamVertex("v6", "B", Properties.create(), t3);
        StreamVertex v7 = new StreamVertex("v7", "A", Properties.create(), t4);
        StreamVertex v8 = new StreamVertex("v8", "B", Properties.create(), t4);

        // Define custom vertex properties
        HashMap<String, Object> propertiesVertexV1 = new HashMap<>();
        propertiesVertexV1.put("Relevance", 1);
        propertiesVertexV1.put("Size", 15);
        propertiesVertexV1.put("Weekday", "Monday");
        Properties propertiesV1 = Properties.createFromMap(propertiesVertexV1);

        HashMap<String, Object> propertiesVertexV2 = new HashMap<>();
        propertiesVertexV2.put("Relevance", 3);
        propertiesVertexV2.put("Size", 10);
        Properties propertiesV2 = Properties.createFromMap(propertiesVertexV2);

        HashMap<String,Object> propertiesWithoutSize = new HashMap<>();
        propertiesWithoutSize.put("Relevance", 2);
        propertiesWithoutSize.put("Weekday","Monday");
        Properties propertiesCustom = Properties.createFromMap(propertiesWithoutSize);

        HashMap<String, Object> propertiesVertexV3 = new HashMap<>();
        propertiesVertexV3.put("Relevance", 2);
        propertiesVertexV3.put("Size", 30);
        propertiesVertexV3.put("Weekday", "Monday");
        Properties propertiesV3 = Properties.createFromMap(propertiesVertexV3);

        HashMap<String, Object> propertiesVertexV4 = new HashMap<>();
        propertiesVertexV4.put("Relevance", 5);
        propertiesVertexV4.put("Size", 5);
        propertiesVertexV4.put("Weekday", "Thursday");
        Properties propertiesV4 = Properties.createFromMap(propertiesVertexV4);


        // Assign vertex properties
        v1.setVertexProperties(propertiesV1);
        v2.setVertexProperties(propertiesV2);
        v3.setVertexProperties(propertiesV3);
        v4.setVertexProperties(propertiesV4);
        v5.setVertexProperties(propertiesCustom);
        v6.setVertexProperties(propertiesV2);
        v7.setVertexProperties(propertiesV3);
        v8.setVertexProperties(propertiesV4);

        // Create custom edge properties
        HashMap<String, Object> propertiesEdge1 = new HashMap<>();
        propertiesEdge1.put("Weekday", "Thursday");
        Properties propertiesE1 = Properties.createFromMap(propertiesEdge1);

        HashMap<String, Object> propertiesEdge2 = new HashMap<>();
        propertiesEdge2.put("Weight", 6);
        Properties propertiesE2 = Properties.createFromMap(propertiesEdge2);

        HashMap<String,Object> propertiesEdge3 = new HashMap<>();
        propertiesEdge3.put("Weekday", "Thursday");
        propertiesEdge3.put("Weight", 3);
        Properties propertiesE3 = Properties.createFromMap(propertiesEdge3);

        // Define edges
        StreamTriple edge1 = new StreamTriple("e1", t1, "impacts",  propertiesE1, v1, v2);
        StreamTriple edge2 = new StreamTriple("e2", t1, "impacts", propertiesE2, v3, v4);
        StreamTriple edge3 = new StreamTriple("e3", t2, "calculates", propertiesE3, v3, v4);
        StreamTriple edge4 = new StreamTriple("e4", t2, "impacts",  propertiesE1, v1, v2);
        StreamTriple edge5 = new StreamTriple("e5", t3, "impacts", propertiesE2, v5, v6);
        StreamTriple edge6 = new StreamTriple("e6", t3, "calculates", propertiesE3, v5, v6);
        StreamTriple edge7 = new StreamTriple("e7", t4, "impacts",  propertiesE1, v7, v8);
        StreamTriple edge8 = new StreamTriple("e8", t4, "impacts", propertiesE2, v7, v8);
        StreamTriple edge9 = new StreamTriple("e9", t1, "calculates", propertiesE3, v7, v8);

        DataStream<StreamTriple> graphStreamTriples = env.fromElements(edge1, edge2, edge3, edge4, edge5, edge6, edge7, edge8, edge9);

        return graphStreamTriples;
    }
}
