package edu.leipzig.application;

import edu.leipzig.impl.algorithm.GraphStreamGrouping;
import edu.leipzig.impl.algorithm.TableGroupingBase;
import edu.leipzig.impl.functions.aggregation.AvgProperty;
import edu.leipzig.impl.functions.aggregation.Count;
import edu.leipzig.impl.functions.aggregation.MinProperty;
import edu.leipzig.impl.functions.aggregation.SumProperty;
import edu.leipzig.model.graph.StreamEdge;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.graph.StreamTriple;
import edu.leipzig.model.graph.StreamVertex;
import edu.leipzig.model.table.TableSet;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.yarn.util.Times;
import org.gradoop.common.model.impl.properties.Properties;

import javax.xml.crypto.Data;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;

public class GraphSummarizationJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Timestamp t1 = new Timestamp(1619511661000L);
        Timestamp t2 = new Timestamp(1619511662000L);
        Timestamp t3 = new Timestamp(1619511673000L);
        Timestamp t4 = new Timestamp(1619511674000L);
        StreamVertex v1 = new StreamVertex("v1", "A", Properties.create(), t1);
        StreamVertex v2 = new StreamVertex("v2", "B", Properties.create(), t1);
        StreamVertex v3 = new StreamVertex("v3", "A", Properties.create(), t2);
        StreamVertex v4 = new StreamVertex("v4", "B", Properties.create(), t2);
        StreamVertex v5 = new StreamVertex("v5", "A", Properties.create(), t3);
        StreamVertex v6 = new StreamVertex("v6", "B", Properties.create(), t3);
        StreamVertex v7 = new StreamVertex("v7", "A", Properties.create(), t4);
        StreamVertex v8 = new StreamVertex("v8", "B", Properties.create(), t4);

        HashMap<String, Object> propertiesVertexV1 = new HashMap<>();
        propertiesVertexV1.put("Relevance", 1);
        propertiesVertexV1.put("Size", 15);
        propertiesVertexV1.put("Weekday", "Monday");
        Properties propertiesV1 = Properties.createFromMap(propertiesVertexV1);

        HashMap<String, Object> propertiesVertexV2 = new HashMap<>();
        propertiesVertexV2.put("Relevance", 3);
        propertiesVertexV2.put("Size", 10);
       //propertiesVertexV2.put("Weekday", "Tuesday");
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

        v1.setVertexProperties(propertiesV1);
        v2.setVertexProperties(propertiesV2);
        v3.setVertexProperties(propertiesV3);
        v4.setVertexProperties(propertiesV4);
        v5.setVertexProperties(propertiesCustom);
        v6.setVertexProperties(propertiesV2);
        v7.setVertexProperties(propertiesV3);
        v8.setVertexProperties(propertiesV4);

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

        StreamGraph streamGraph = StreamGraph.fromFlinkStream(graphStreamTriples, new StreamGraphConfig(env));


        TableGroupingBase.GroupingBuilder groupingBuilder = new TableGroupingBase.GroupingBuilder();

        groupingBuilder.addVertexGroupingKey(":label");
        groupingBuilder.addEdgeGroupingKey(":label");
        groupingBuilder.addVertexAggregateFunction(new Count());
        groupingBuilder.addVertexAggregateFunction(new Count());
        groupingBuilder.addEdgeAggregateFunction(new Count());

        streamGraph = groupingBuilder.build().execute(streamGraph);

        DataStream<StreamTriple> tripleDataStream = streamGraphToStreamTriple(streamGraph);

        tripleDataStream.print();

        //minimalisticExample(env, streamGraph, graphStreamTriples);
        env.execute();
    }

    public static DataStream<StreamTriple> streamGraphToStreamTriple(StreamGraph streamGraph){
        StreamTableEnvironment tEnv = streamGraph.getConfig().getTableEnvironment();
        Table edges = streamGraph.getTableSet().getEdges();
        Table vertices = streamGraph.getTableSet().getVertices();

        Table joinedTableEdgesVertices = streamGraph.createStreamTriple(vertices, edges);
        DataStream<Row> rowStreamEdgesVertices = tEnv.toDataStream(tEnv.sqlQuery("SELECT * FROM " + joinedTableEdgesVertices));

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

    public static void minimalisticExample(StreamExecutionEnvironment env,
      StreamGraph streamGraph, DataStream<StreamTriple> graphTriples) throws Exception {

        StreamTableEnvironment tEnv = streamGraph.getConfig().getTableEnvironment();

        Timestamp t1 = new Timestamp(1619511661000L);

        Tuple3<String, Timestamp, Integer> t31 = new Tuple3<>("A", t1, 1);
        Tuple3<String, Timestamp, Integer> t32 = new Tuple3<>("B", t1, 2);
        Tuple3<String, Timestamp, Integer> t33 = new Tuple3<>("A", t1, 3);
        Tuple3<String, Timestamp, Integer> t34 = new Tuple3<>("B", t1, 4);

        Tuple4<String, Timestamp, Integer, Float> t41 = new Tuple4<>("A", t1, 10, 15.6F);
        Tuple4<String, Timestamp, Integer, Float> t42 = new Tuple4<>("B", t1, 20, 15.5F);
        Tuple4<String, Timestamp, Integer, Float> t43 = new Tuple4<>("A", t1, 30, 13.5F);
        Tuple4<String, Timestamp, Integer, Float> t44 = new Tuple4<>("B", t1, 40, 19.6F);


        StreamGraphConfig config = new StreamGraphConfig(env);

        DataStream<Tuple3<String, Timestamp, Integer>> stream3 = env.fromElements(t31, t32, t33, t34);
        DataStream<Tuple4<String, Timestamp, Integer, Float>> stream4 = env.fromElements(t41, t42, t43, t44);

        stream3 = stream3.assignTimestampsAndWatermarks(
          WatermarkStrategy.<Tuple3<String, Timestamp, Integer>>forBoundedOutOfOrderness(config.getMaxOutOfOrdernessDuration())
            .withTimestampAssigner((event, timestamp) -> event.f1.getTime()));

        stream4 = stream4.assignTimestampsAndWatermarks(
          WatermarkStrategy.<Tuple4<String, Timestamp, Integer, Float>>forBoundedOutOfOrderness(config.getMaxOutOfOrdernessDuration())
            .withTimestampAssigner((event, timestamp) -> event.f1.getTime()));


        DataStream<Integer> extendedJoinedStream = stream3.join(stream4).where(s3 -> "A").equalTo(s4 -> "A")
          .window(TumblingEventTimeWindows.of(Time.seconds(10))).apply(
            new JoinFunction<Tuple3<String, Timestamp, Integer>, Tuple4<String, Timestamp, Integer, Float>, Integer>() {
                @Override
                public Integer join(Tuple3<String, Timestamp, Integer> stringTimestampIntegerTuple3, Tuple4<String, Timestamp, Integer, Float> stringTimestampIntegerFloatTuple4) throws
                  Exception {
                    return 1;
                }
            });

        extendedJoinedStream.print();

        Table edges = streamGraph.getTableSet().getEdges();
        Table vertices = streamGraph.getTableSet().getVertices();

        //---------- ERSTELLE DATASTREAMS MIT WATERMARKS ----------

        DataStream<Tuple2<Boolean, StreamEdge>> edgeDataStream = tEnv.toRetractStream(edges, StreamEdge.class)
          .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Tuple2<Boolean, StreamEdge>>forBoundedOutOfOrderness(config.getMaxOutOfOrdernessDuration())
              .withTimestampAssigner((event, timestamp) -> event.f1.getEventTime().getTime()));

        DataStream<Tuple2<Boolean, StreamVertex>> vertexDataStream =
          tEnv.toRetractStream(vertices, StreamVertex.class).assignTimestampsAndWatermarks(
            WatermarkStrategy.<Tuple2<Boolean, StreamVertex>>forBoundedOutOfOrderness(config.getMaxOutOfOrdernessDuration())
              .withTimestampAssigner((event, timestamp) -> event.f1.getEventTime().getTime()));

        graphTriples = graphTriples.assignTimestampsAndWatermarks(
          WatermarkStrategy.<StreamTriple>forBoundedOutOfOrderness(config.getMaxOutOfOrdernessDuration())
            .withTimestampAssigner((event, timestamp) -> event.f1.getTime()));


        // ---------- JOIN EDGES UND VERTICES ----------

        DataStream<Integer> joinedStreamEdgeVertex =
          edgeDataStream.join(vertexDataStream).where(e -> "A").equalTo(v -> "A").window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .apply(new JoinFunction<Tuple2<Boolean, StreamEdge>, Tuple2<Boolean, StreamVertex>, Integer>() {
                @Override
                public Integer join(Tuple2<Boolean, StreamEdge> booleanStreamEdgeTuple2, Tuple2<Boolean, StreamVertex> booleanStreamVertexTuple2) throws Exception {
                    return 2;
                }
            });
        joinedStreamEdgeVertex.print();

        //---------- JOIN EDGES UND STREAMTRIPLE ----------

        DataStream<Integer> joinedStreamEdgeTriple =
          edgeDataStream.join(graphTriples).where(e -> "A").equalTo(t -> "A").window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .apply(new JoinFunction<Tuple2<Boolean, StreamEdge>, StreamTriple, Integer>() {
                @Override
                public Integer join(Tuple2<Boolean, StreamEdge> booleanStreamEdgeTuple2, StreamTriple streamTriple) throws Exception {
                    return 3;
                }
            });
        joinedStreamEdgeTriple.print();

        //---------- JOIN VERTICES UND STREAMTRIPLE ----------

        DataStream<Integer> joinedStreamVertexTriple =
          vertexDataStream.join(graphTriples).where(v -> "A").equalTo(t -> "A").window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .apply(new JoinFunction<Tuple2<Boolean, StreamVertex>, StreamTriple, Integer>() {
                @Override
                public Integer join(Tuple2<Boolean, StreamVertex> booleanStreamVertexTuple2, StreamTriple streamTriple) throws Exception {
                    return 4;
                }
            });
        joinedStreamVertexTriple.print();

        //---------- UMWANDLUNG EDGES ZU TUPLE6 ----------

        DataStream<Tuple6<String, String, Properties, String, String, Timestamp>> edgesAsTuples =
          edgeDataStream.map(new MapFunction<Tuple2<Boolean, StreamEdge>, Tuple6<String, String, Properties, String, String, Timestamp>>() {
              @Override
              public Tuple6<String, String, Properties, String, String, Timestamp> map(Tuple2<Boolean, StreamEdge> booleanStreamEdgeTuple2) throws Exception {
                  StreamEdge edge = booleanStreamEdgeTuple2.f1;
                  return new Tuple6<>(edge.getEdgeId(), edge.getEdgeLabel(), edge.getEdgeProperties(), edge.getSourceId(), edge.getTargetId(), edge.getEventTime());
              }
          });

        //---------- UMWANDLUNG VERTEX ZU TUPLE4 ----------

        DataStream<Tuple4<String, String, Properties, Timestamp>> verticesAsTuples = vertexDataStream.map(
          new MapFunction<Tuple2<Boolean, StreamVertex>, Tuple4<String, String, Properties, Timestamp>>() {
              @Override
              public Tuple4<String, String, Properties, Timestamp> map(
                Tuple2<Boolean, StreamVertex> booleanStreamVertexTuple2) throws Exception {
                  StreamVertex vertex = booleanStreamVertexTuple2.f1;
                  return new Tuple4<>(vertex.getVertexId(), vertex.getVertexLabel(),
                    vertex.getVertexProperties(), vertex.getEventTime());
              }
          });

        //---------- JOINEN DER VERTEX UND EDGE TUPLES ----------

        DataStream<Integer> joinedTuples =
          verticesAsTuples.join(edgesAsTuples).where(v -> "A").equalTo(e -> "A").window(TumblingEventTimeWindows.of(Time.seconds(10))).apply(
            new JoinFunction<Tuple4<String, String, Properties, Timestamp>, Tuple6<String, String, Properties, String, String, Timestamp>, Integer>() {
                @Override
                public Integer join(Tuple4<String, String, Properties, Timestamp> stringStringPropertiesTimestampTuple4,
                  Tuple6<String, String, Properties, String, String, Timestamp> stringStringPropertiesStringStringTimestampTuple6) throws
                  Exception {
                    return 6;
                }
            });

        joinedTuples.print();

        //---------- UMWANDELN DER EDGES ZU STREAMTRIPELN ----------

        DataStream<StreamTriple> edgesAsTriple = edgeDataStream.map(new MapFunction<Tuple2<Boolean,
          StreamEdge>, StreamTriple>() {
            @Override
            public StreamTriple map(Tuple2<Boolean, StreamEdge> booleanStreamEdgeTuple2) throws Exception {
                StreamEdge edge = booleanStreamEdgeTuple2.f1;
                StreamVertex sourceVertex = new StreamVertex(edge.getSourceId(), null, null, null);
                StreamVertex targetVertex = new StreamVertex(edge.getTargetId(), null, null, null);
                return new StreamTriple(edge.getEdgeId(), edge.getEventTime(), edge.getEdgeLabel(), edge.getEdgeProperties(), sourceVertex, targetVertex);
            }
        });

        //---------- JOINEN DER VERTICES UND EDGE-STREAMTRIPLE ---------
        DataStream<Integer> joinVertexEdgeStreamTriple =
          vertexDataStream.join(edgesAsTriple).where(v -> "A").equalTo(e -> "A").window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .apply(new JoinFunction<Tuple2<Boolean, StreamVertex>, StreamTriple, Integer>() {
                @Override
                public Integer join(Tuple2<Boolean, StreamVertex> booleanStreamVertexTuple2, StreamTriple streamTriple) throws Exception {
                    return 5;
                }
            });
        joinVertexEdgeStreamTriple.print();
    }
}
