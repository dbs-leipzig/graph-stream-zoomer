package edu.leipzig.impl.algorithm;

import edu.leipzig.impl.functions.aggregation.AvgProperty;
import edu.leipzig.impl.functions.aggregation.MaxProperty;
import edu.leipzig.impl.functions.aggregation.MinProperty;
import edu.leipzig.impl.functions.aggregation.SumProperty;
import edu.leipzig.impl.functions.utils.CreateSuperElementId;
import edu.leipzig.model.graph.StreamEdge;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.graph.StreamTriple;
import edu.leipzig.model.graph.StreamVertex;
import edu.leipzig.model.table.TableSet;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.gradoop.common.model.impl.properties.Properties;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static edu.leipzig.model.table.TableSet.*;
import static org.apache.flink.table.api.Expressions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraphStreamGroupingTest {

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
    new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberSlotsPerTaskManager(2)
        .setNumberTaskManagers(1)
        .build());


  private static class CollectVertexSink implements SinkFunction<Tuple2<Boolean, StreamVertex>> {

    // must be static
    public static final Map<String, StreamVertex> values = Collections.synchronizedMap(new HashMap<>());

    @Override
    public void invoke(Tuple2<Boolean, StreamVertex> value, SinkFunction.Context context) throws Exception {
      if (value.f0) {
        values.put(value.f1.getVertexId(), value.f1);
      } else {
        values.remove(value.f1.getVertexId());
      }

    }
  }

  private static class CollectEdgeSink implements SinkFunction<Tuple2<Boolean, StreamEdge>> {

    // must be static
    public static final Map<String, StreamEdge> values = Collections.synchronizedMap(new HashMap<>());

    @Override
    public void invoke(Tuple2<Boolean, StreamEdge> value, SinkFunction.Context context) throws Exception {
      if (value.f0) {
        values.put(value.f1.getEdgeId(), value.f1);
      } else {
        values.remove(value.f1.getEdgeId());
      }

    }
  }
  @Test
  public void testDoubleAliasingOnlyWithSqlQuery() {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final EnvironmentSettings bsSettings =
      EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    final StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, bsSettings);

    Timestamp t1 = new Timestamp(1619511611000L);
    Timestamp t2 = new Timestamp(1619511621000L);
    Timestamp t3 = new Timestamp(1619511631000L);
    Timestamp t4 = new Timestamp(1619511641000L);
    Timestamp t5 = new Timestamp(1619511651000L);
    Timestamp t6 = new Timestamp(1619511661000L);
    Timestamp t7 = new Timestamp(1619511671000L);
    Timestamp t8 = new Timestamp(1619511681000L);
    Timestamp t9 = new Timestamp(1619511691000L);
    Timestamp t10 = new Timestamp(1619511601000L);

    Tuple3<String, String, Timestamp> tp1 = new Tuple3<>("1", "A", t1);
    Tuple3<String, String, Timestamp> tp2 = new Tuple3<>("2", "A", t1);
    Tuple3<String, String, Timestamp> tp3 = new Tuple3<>("3", "B", t2);
    Tuple3<String, String, Timestamp> tp4 = new Tuple3<>("4", "B", t3);
    Tuple3<String, String, Timestamp> tp5 = new Tuple3<>("5", "C", t1);
    Tuple3<String, String, Timestamp> tp6 = new Tuple3<>("6", "C", t4);
    Tuple3<String, String, Timestamp> tp7 = new Tuple3<>("7", "D", t3);
    Tuple3<String, String, Timestamp> tp8 = new Tuple3<>("8", "D", t2);
    Tuple3<String, String, Timestamp> tp9 = new Tuple3<>("9", "E", t5);
    Tuple3<String, String, Timestamp> tp10= new Tuple3<>("10", "E", t1);

    Table vertices = streamTableEnvironment.fromDataStream(
      env
        .fromElements(tp1, tp2, tp3, tp4, tp5, tp6, tp7, tp8, tp9, tp10)
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Timestamp>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
          .withTimestampAssigner((event, timestamp) -> event.f2.getTime())),
      $("f0").as("name"), $("f1").as("label"), $("f2").rowtime().as("timestamp1"));
    vertices.execute().print();


    Table modifiedVertices = vertices.window(Tumble.over(lit(20).seconds()).on($("timestamp1")).as("tmp"))
      .groupBy($("name"), $("label"), $("tmp"))
      .select($("name"), $("label"), $("tmp").rowtime().as("timestampv2"));

    System.out.println(modifiedVertices.execute().getResolvedSchema().toString());


    /*
    Table modifiedVertices1 = streamTableEnvironment.sqlQuery("Select window_start, window_end, name, label" +
      " FROM TABLE ( TUMBLE( TABLE " + vertices + ", DESCRIPTOR(timestamp1), INTERVAL '10' SECONDS)) GROUP " +
      "BY " +
      "window_start, window_end, name, label");
    modifiedVertices1.execute().print();

    Table modifiedVertices2 = streamTableEnvironment.sqlQuery("SELECT TUMBLE_START(timestamp1, INTERVAL " +
      "'10' SECONDS) as w1_rowtime, name, label FROM " + vertices + " GROUP BY TUMBLE(timestamp1, INTERVAL " +
      "'10' SECONDS)," +
      " name, label");
    modifiedVertices2.execute().print();
/*
    Table groupedModifiedVertices = modifiedVertices1.window(Tumble.over(lit(10).seconds()).on($(
      "timestampv2")).as("tmp"))
      .groupBy($("label"), $("tmp"))
      .select($("label").as("super"), $("tmp").rowtime().as("timestampv4"));

 */

    Table groupedModifiedVertices1 = streamTableEnvironment.sqlQuery("Select window_start, window_end, " +
      "window_time, " +
      "label" +
      " as super_label, count(*) as amount FROM TABLE ( TUMBLE ( TABLE " + modifiedVertices + ", DESCRIPTOR" +
      "(timestampv2), INTERVAL '20' " +
      "SECONDS)) GROUP BY window_start, window_end, label, window_time");


    //groupedModifiedVertices.execute().print();
    /*
    Table groupedModifiedVertices1 = streamTableEnvironment.sqlQuery("SELECT TUMBLE_START(timestampv2, " +
      "INTERVAL '20' SECONDS) as " +
      "w2_rowtime, label as super_label, count(*) as amount FROM " + modifiedVertices + " GROUP BY TUMBLE" +
      "(timestampv2, INTERVAL" +
      " '20' SECONDS), label");

     */
    System.out.println(groupedModifiedVertices1.execute().getResolvedSchema().toString());

    Table groupedVerticesTest = streamTableEnvironment.sqlQuery("SELECT TUMBLE_START(w2_rowtime, " +
      "INTERVAL '20' SECONDS) as " +
      "w3_rowtime, super_label FROM " + groupedModifiedVertices1 + " GROUP BY " +
      "TUMBLE" +
      "(w2_rowtime, INTERVAL" +
      " '20' SECONDS), super_label");

    streamTableEnvironment.sqlQuery("SELECT w2_rowtime, super_label FROM " + groupedModifiedVertices1);
    groupedVerticesTest.execute().print();
    System.out.println(groupedVerticesTest.getResolvedSchema());



  }


  @Test
  public void textDoubleAliasing() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final EnvironmentSettings bsSettings =
      EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    final StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, bsSettings);

    Timestamp t1 = new Timestamp(1619511681000L);
    Timestamp t2 = new Timestamp(1619511682000L);
    Timestamp t3 = new Timestamp(1619511683000L);
    Timestamp t4 = new Timestamp(1619511684000L);
    Timestamp t5 = new Timestamp(1619511685000L);
    Timestamp t6 = new Timestamp(1619511686000L);
    Timestamp t7 = new Timestamp(1619511687000L);
    Timestamp t8 = new Timestamp(1619511688000L);
    Timestamp t9 = new Timestamp(1619511689000L);
    Timestamp t10 = new Timestamp(161951160000L);

    Tuple3<String, String, Timestamp> tp1 = new Tuple3<>("A", "A", t1);
    Tuple3<String, String, Timestamp> tp2 = new Tuple3<>("B", "A", t2);
    Tuple3<String, String, Timestamp> tp3 = new Tuple3<>("C", "B", t3);
    Tuple3<String, String, Timestamp> tp4 = new Tuple3<>("D", "B", t4);
    Tuple3<String, String, Timestamp> tp5 = new Tuple3<>("E", "C", t5);
    Tuple3<String, String, Timestamp> tp6 = new Tuple3<>("F", "C", t6);
    Tuple3<String, String, Timestamp> tp7 = new Tuple3<>("G", "D", t7);
    Tuple3<String, String, Timestamp> tp8 = new Tuple3<>("H", "D", t8);
    Tuple3<String, String, Timestamp> tp9 = new Tuple3<>("I", "E", t9);
    Tuple3<String, String, Timestamp> tp10= new Tuple3<>("J", "E", t10);

    Table vertices = streamTableEnvironment.fromDataStream(
      env
        .fromElements(tp1, tp2, tp3, tp4, tp5, tp6, tp7, tp8, tp9, tp10)
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Timestamp>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
          .withTimestampAssigner((event, timestamp) -> event.f2.getTime())),
      $("f0").as("name"), $("f1").as("label"), $("f2").rowtime().as("timestamp"));
    vertices.execute().print();


    Table modifiedVertices = vertices.window(Tumble.over(lit(10).seconds()).on($("timestamp")).as("tmp"))
    .groupBy($("name"), $("label"), $("tmp"))
      .select($("name"), $("label"), $("tmp").rowtime().as("timestampv2"));

    modifiedVertices.execute().print();

    Table modifiedVertices1 =
      modifiedVertices.select($("name"), $("label"), $("timestampv2").as("timestampv3"));


    Table groupedModifiedVertices = modifiedVertices1.window(Tumble.over(lit(10).seconds()).on($(
      "timestampv3")).as("tmp"))
    .groupBy($("label"), $("tmp"))
      .select($("label").as("super"), $("tmp").rowtime().as("timestampv4"));

    /*groupedModifiedVertices.select($("super_label"), $("timestampv4")); <--- SUPER WEIRD BUG! Cannot
    resolve field [super_label], input field list:[super_Label, timestampv4]. Beachte: Kein .execute, nur
    select
     */

    groupedModifiedVertices.select($("super"), $("timestampv4")).execute().print();
  }

  @Test
  public void testDoubleGrouping() throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    final StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, bsSettings);

    Timestamp t1 = new Timestamp(1619511661000L);
    Timestamp t2 = new Timestamp(1619511672000L);
    Timestamp t3 = new Timestamp(1619511683000L);
    Timestamp t4 = new Timestamp(1619511694000L);

    // Input data, StreamVertex(vertex_id, vertex_label, vertex_properties, event_time)
    StreamVertex v11 = new StreamVertex("v1", "A", Properties.create(), t1);
    StreamVertex v12 = new StreamVertex("v2", "B", Properties.create(), t1);
    StreamVertex v21 = new StreamVertex("v1", "A", Properties.create(), t2);
    StreamVertex v22 = new StreamVertex("v2", "B", Properties.create(), t2);
    StreamVertex v31 = new StreamVertex("v1", "A", Properties.create(), t3);
    StreamVertex v32 = new StreamVertex("v2", "B", Properties.create(), t3);
    StreamVertex v41 = new StreamVertex("v1", "A", Properties.create(), t4);
    StreamVertex v42 = new StreamVertex("v2", "B", Properties.create(), t4);
    HashMap<String, Object> propertiesMap = new HashMap<>();
    propertiesMap.put("Relevanz", 1);
    propertiesMap.put("Größe", 15);
    propertiesMap.put("Name", "Dieter");
    propertiesMap.put("ErsatzId", 1523213L);
    Properties propertiesToSet = Properties.createFromMap(propertiesMap);
    v11.setVertexProperties(propertiesToSet);

    String ID = FIELD_VERTEX_ID;
    String LABEL = FIELD_VERTEX_LABEL;
    String PROPERTIES = FIELD_VERTEX_PROPERTIES;
    String EVENT_TIME = FIELD_EVENT_TIME;

    CreateSuperElementId f1 = new CreateSuperElementId();

    streamTableEnvironment.registerFunction(f1.toString(), f1);

    // Put input data to table
    Table vertices = streamTableEnvironment.fromDataStream(
      env
        .fromElements(v11, v12, v21, v22, v31, v32, v41, v42)
        .assignTimestampsAndWatermarks(WatermarkStrategy.<StreamVertex>forBoundedOutOfOrderness(Duration.ofSeconds(5))
          .withTimestampAssigner((event, timestamp) -> event.getEventTime().getTime())),
      // Expressions with declaration of 'event_time' as rowtime
      $(ID), $(LABEL), $(PROPERTIES), $(EVENT_TIME).rowtime().as("w1_rowtime"));
    vertices.execute().print();
    System.out.println(vertices.getResolvedSchema().toString());



// 1. Prepare distinct vertices
    Table preparedVertices = vertices
      .window(Tumble.over(lit(20).seconds()).on($("w1_rowtime")).as("w1"))
      .groupBy($(ID), $(LABEL), $("w1"))
      .select($(ID), $(LABEL), lit(5L).as("prop"), $("w1").rowtime().as("w1_rowtime"));


    Table groupedVertices = streamTableEnvironment.sqlQuery("SELECT  " +
      "window_time as w2_rowtime, " +
      " vertex_label as super_label, count(*) as super_count  FROM TABLE ( TUMBLE ( TABLE " + preparedVertices +
      " , DESCRIPTOR(w1_rowtime), INTERVAL '20' SECONDS)) GROUP BY window_start, window_end, window_time, " +
      "vertex_label");

    groupedVertices.execute().print();

    groupedVertices
      .select($("super_label"), $("w2_rowtime"))
      .execute().print();

    System.out.println(groupedVertices.execute().getResolvedSchema());
  }

  @Test
  public void testPerformGroupingMethod(){
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //Difference between timestamps: 10 seconds
    Timestamp t1 = new Timestamp(1619511661000L);
    Timestamp t2 = new Timestamp(1619511672000L);
    Timestamp t3 = new Timestamp(1619511683000L);
    Timestamp t4 = new Timestamp(1619511694000L);
    StreamVertex v1 = new StreamVertex("v1", "A", Properties.create(), t1);
    StreamVertex v2 = new StreamVertex("v2", "B", Properties.create(), t2);
    StreamVertex v3 = new StreamVertex("v3", "A", Properties.create(), t1);
    StreamVertex v4 = new StreamVertex("v4", "B", Properties.create(), t2);

    System.out.println(v1.getEventTime() + " " + v2.getEventTime() + " " + v3.getEventTime() + " " + v4.getEventTime());
    HashMap<String, Object> propertiesVertexV1 = new HashMap<>();
    propertiesVertexV1.put("Relevance", 1);
    propertiesVertexV1.put("Size", 15);
    Properties propertiesV1 = Properties.createFromMap(propertiesVertexV1);
    v1.setVertexProperties(propertiesV1);

    HashMap<String, Object> propertiesVertexV2 = new HashMap<>();
    propertiesVertexV2.put("Relevance", 3);
    propertiesVertexV2.put("Size", 10);
    Properties propertiesV2 = Properties.createFromMap(propertiesVertexV2);
    v2.setVertexProperties(propertiesV2);

    HashMap<String, Object> propertiesVertexV3 = new HashMap<>();
    propertiesVertexV3.put("Relevance", 2);
    propertiesVertexV3.put("Size", 30);
    Properties propertiesV3 = Properties.createFromMap(propertiesVertexV3);
    v3.setVertexProperties(propertiesV3);

    HashMap<String, Object> propertiesVertexV4 = new HashMap<>();
    propertiesVertexV4.put("Relevance", 5);
    propertiesVertexV4.put("Size", 5);
    Properties propertiesV4 = Properties.createFromMap(propertiesVertexV4);
    v4.setVertexProperties(propertiesV4);

    HashMap<String, Object> propertiesEdge1 = new HashMap<>();
    propertiesEdge1.put("Weight", 5);
    Properties propertiesE1 = Properties.createFromMap(propertiesEdge1);

    HashMap<String, Object> propertiesEdge2 = new HashMap<>();
    propertiesEdge2.put("Weight", 6);
    Properties propertiesE2 = Properties.createFromMap(propertiesEdge2);

    StreamTriple edge1 = new StreamTriple("1", t1, "impacts",  propertiesE1, v1, v2);
    StreamTriple edge2 = new StreamTriple("2", t2, "impacts", propertiesE2, v3, v4);

    DataStream<StreamTriple> testStream = env.fromElements(edge1, edge2);
    StreamGraph streamGraph = StreamGraph.fromFlinkStream(testStream, new StreamGraphConfig(env));

    GraphStreamGrouping groupingOperator = new TableGroupingBase.GroupingBuilder()
      .addVertexGroupingKey(":label")
      .addVertexAggregateFunction(new MinProperty("Relevance"))
      .addVertexAggregateFunction(new AvgProperty("Size"))
      .addEdgeGroupingKey(":label")
      .addEdgeAggregateFunction(new SumProperty("Weight"))
      .build();

    streamGraph = groupingOperator.execute(streamGraph);
    streamGraph.print();

  }
}