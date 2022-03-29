package edu.leipzig.impl.algorithm;

import edu.leipzig.impl.functions.aggregation.AvgProperty;
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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import edu.leipzig.impl.algorithm.TableGroupingBase;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.model.impl.operators.aggregation.functions.average.Average;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import scala.Dynamic;
import scala.Serializable;

import javax.xml.crypto.Data;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.leipzig.model.table.TableSet.*;
import static org.apache.flink.table.api.Expressions.*;
import static org.junit.Assert.assertTrue;

/**
 * TODO: These unit/integration tests are messy
 * Edges with same source and target vertex but different labels lead to double vertex entry in vertices
 * table
 */
public class GraphStreamGroupingTest {

  private StreamVertex v1, v2, v3, v4, v5, v6, v7, v8;
  private StreamTriple edge1, edge2, edge3, edge4, edge5, edge6, edge7, edge8, edge9;
  private Timestamp t1, t2, t3, t4;
  private HashMap<String, Object> propertiesVertexV1, propertiesVertexV2, propertiesVertexV3,
    propertiesVertexV4;
  HashMap<String, Object> propertiesEdge1,propertiesEdge2, propertiesEdge3;
  private Properties propertiesV1, propertiesV2, propertiesV3, propertiesV4;
  private Properties propertiesE1, propertiesE2, propertiesE3;
  private DataStream<StreamTriple> testStream;
  private StreamGraph streamGraph;
  TableEnvironment tEnv;
  Table vertices, edges;
  int windowSize;
  TableGroupingBase.GroupingBuilder groupingBuilder;
  GraphStreamGrouping graphStreamGrouping;
  StreamTableEnvironment streamTableEnvironment;
  final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
    new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberSlotsPerTaskManager(2)
        .setNumberTaskManagers(1)
        .build());

  @Before
  public void init(){
    EnvironmentSettings settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build();
    tEnv = TableEnvironment.create(settings);

    windowSize = 10;

    t1 = new Timestamp(1619511661000L);
    t2 = new Timestamp(1619511662000L);
    t3 = new Timestamp(1619511673000L);
    t4 = new Timestamp(1619511674000L);

    v1 = new StreamVertex("v1", "A", Properties.create(), t1);
    v2 = new StreamVertex("v2", "B", Properties.create(), t2);
    v3 = new StreamVertex("v3", "A", Properties.create(), t3);
    v4 = new StreamVertex("v4", "B", Properties.create(), t4);
    v5 = new StreamVertex("v5", "A", Properties.create(), t1);
    v6 = new StreamVertex("v6", "B", Properties.create(), t2);
    v7 = new StreamVertex("v7", "A", Properties.create(), t3);
    v8 = new StreamVertex("v8", "B", Properties.create(), t4);

    propertiesVertexV1 = new HashMap<>();
    propertiesVertexV1.put("Relevance", 1);
    propertiesVertexV1.put("Size", 15);
    propertiesVertexV1.put("Weekday", "Monday");
    propertiesV1 = Properties.createFromMap(propertiesVertexV1);

    propertiesVertexV2 = new HashMap<>();
    propertiesVertexV2.put("Relevance", 3);
    propertiesVertexV2.put("Size", 15);
    propertiesVertexV2.put("Weekday", "Tuesday");
    propertiesV2 = Properties.createFromMap(propertiesVertexV2);


    propertiesVertexV3 = new HashMap<>();
    propertiesVertexV3.put("Relevance", 2);
    propertiesVertexV3.put("Size", 30);
    propertiesVertexV3.put("Weekday", "Monday");
    propertiesV3 = Properties.createFromMap(propertiesVertexV3);

    propertiesVertexV4 = new HashMap<>();
    propertiesVertexV4.put("Relevance", 5);
    propertiesVertexV4.put("Size", 5);
    propertiesVertexV4.put("Weekday", "Thursday");
    propertiesV4 = Properties.createFromMap(propertiesVertexV4);

    v1.setVertexProperties(propertiesV1);
    v2.setVertexProperties(propertiesV2);
    v3.setVertexProperties(propertiesV3);
    v4.setVertexProperties(propertiesV4);
    v5.setVertexProperties(propertiesV1);
    v6.setVertexProperties(propertiesV2);
    v7.setVertexProperties(propertiesV3);
    v8.setVertexProperties(propertiesV4);

    propertiesEdge1 = new HashMap<>();
    propertiesEdge1.put("Weight", 5);
    propertiesEdge1.put("Weekday", "Thursday");
    propertiesE1 = Properties.createFromMap(propertiesEdge1);

    propertiesEdge2 = new HashMap<>();
    propertiesEdge2.put("Weight", 6);
    propertiesEdge2.put("Weekday", "Wednesday");
    propertiesE2 = Properties.createFromMap(propertiesEdge2);

    propertiesEdge3 = new HashMap<>();
    propertiesEdge3.put("Weekday", "Thursday");
    propertiesEdge3.put("Weight", 3);
    propertiesE3 = Properties.createFromMap(propertiesEdge3);

    edge1 = new StreamTriple("e1", t1, "impacts",  propertiesE1, v1, v2);
    edge2 = new StreamTriple("e2", t1, "impacts", propertiesE2, v3, v4);
    edge3 = new StreamTriple("e3", t1, "calculates", propertiesE3, v3, v4);
    edge4 = new StreamTriple("e4", t1, "impacts",  propertiesE1, v1, v2);
    edge5 = new StreamTriple("e5", t1, "impacts", propertiesE2, v5, v6);
    edge6 = new StreamTriple("e6", t1, "calculates", propertiesE3, v5, v6);
    edge7 = new StreamTriple("e7", t1, "impacts",  propertiesE1, v7, v8);
    edge8 = new StreamTriple("e8", t1, "impacts", propertiesE2, v7, v8);
    edge9 = new StreamTriple("e9", t1, "calculates", propertiesE3, v7, v8);

    testStream = env.fromElements(edge1, edge2, edge3,  edge5, edge6, edge7, edge8, edge9);

    streamGraph = StreamGraph.fromFlinkStream(testStream, new StreamGraphConfig(env));

    vertices = streamGraph.getTableSet().getVertices();
    edges = streamGraph.getTableSet().getEdges();

    groupingBuilder = new TableGroupingBase.GroupingBuilder();
    groupingBuilder.addVertexGroupingKey(":label");
    groupingBuilder.addVertexGroupingKey("Weekday");
    groupingBuilder.addVertexAggregateFunction(new SumProperty("Relevance"));
    groupingBuilder.addVertexAggregateFunction(new AvgProperty("Size"));
    groupingBuilder.addEdgeGroupingKey(":label");
    groupingBuilder.addEdgeGroupingKey("Weekday");
    groupingBuilder.addEdgeAggregateFunction(new MinProperty("Weight"));
    graphStreamGrouping = groupingBuilder.build();

    //Wichtig, da sonst graphStreamGrouping.getTableEnv() und .getConfig() null sind
    streamGraph = graphStreamGrouping.execute(streamGraph);
    streamTableEnvironment = graphStreamGrouping.getTableEnv();


  }

 class StreamToHashMap implements  MapFunction<Row, HashMap<String, String>>  {
    List<String> localVertexGroupingPropertyKeys;
    boolean localUseVertexLabel;
    StreamToHashMap(boolean localUseVertexLabel, List<String> localVertexGroupingPropertyKeys){
      //Create local instance of variables of outer class so inner method is serializable
      this.localVertexGroupingPropertyKeys = localVertexGroupingPropertyKeys;
      this.localUseVertexLabel = localUseVertexLabel;
    }
    public HashMap<String,String> map(Row row){
      HashMap<String, String> toReturn = new HashMap<>();

      /*
      Create keys from values of vertex grouping properties
      E.g. label, weekday -> key: A, Monday, timestamp
       */
      String key = "";
      Properties vertexPropertyContent = (Properties) row.getField("vertex_properties");
      if (localUseVertexLabel) {
        key += row.getField("vertex_label").toString();
      }
      for (String s : localVertexGroupingPropertyKeys) {
        if (key.equals("")) {
          key = vertexPropertyContent.get("\""+s+"\"").toString();
        }
        else {
          key += ", " + vertexPropertyContent.get("\""+s+"\"").toString();
        }
      }
      key += ", " +  row.getField("vertex_event_time").toString().replaceAll("T", " ");

      /*
      Value of the generated key is every property of the vertex that is not contained in the key already
       */
      String value = "";
      if (!localUseVertexLabel){
        value = row.getField("vertex_properties").toString();
      }
      for (Property p : vertexPropertyContent) {
        if (!localVertexGroupingPropertyKeys.contains(p.getKey().replaceAll("\"", ""))) {
          if (value.equals("")) {
            value = p.getKey() + ":" + p.getValue();
          }
          else {
            value += "; " + p.getKey() + ":" + p.getValue();
          }
        }
      }
      toReturn.put(key, value);
      return toReturn;
    }
  };


  @Test
  public void testPrepareVerticesMethod() throws Exception{
    Table prepareVertices = graphStreamGrouping.prepareVertices(streamGraph.getTableSet(),
      streamTableEnvironment);
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(prepareVertices);
    final HashMap<String, String> hashMapVertices = new HashMap<>();
    List<HashMap<String,String>> resultingMaps =
      rowStream.map(new StreamToHashMap(graphStreamGrouping.useVertexLabels,
      graphStreamGrouping.vertexGroupingPropertyKeys)).executeAndCollect(10);
    for (HashMap<String, String> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapVertices.put(key, result.get(key));
      }
    }

    Timestamp TsTot1 = new Timestamp(t1.getTime());
    Timestamp TsTot2 = new Timestamp(t2.getTime());
    Timestamp TsTot3 = new Timestamp(t3.getTime());
    Timestamp TsTot4 = new Timestamp(t4.getTime());

    TsTot1.setSeconds((TsTot1.getSeconds()/10+1) * 10);
    TsTot2.setSeconds((TsTot2.getSeconds()/10+1) * 10);
    TsTot3.setSeconds((TsTot3.getSeconds()/10+1) * 10);
    TsTot4.setSeconds((TsTot4.getSeconds()/10+1) * 10);

    Timestamp windowOfT1 = new Timestamp(TsTot1.getTime() - 1);
    Timestamp windowOfT2 = new Timestamp(TsTot2.getTime() - 1);
    Timestamp windowOfT3 = new Timestamp(TsTot3.getTime() - 1);
    Timestamp windowOfT4 = new Timestamp(TsTot4.getTime() - 1);

    String propertiesV1ToCompare = hashMapVertices.get(v1.getVertexLabel() + ", Monday, " + windowOfT1);
    String propertiesV2ToCompare = hashMapVertices.get(v2.getVertexLabel() + ", Tuesday, "+ windowOfT2);
    String propertiesV3ToCompare = hashMapVertices.get(v3.getVertexLabel() + ", Monday, " + windowOfT3);
    String propertiesV4ToCompare = hashMapVertices.get(v4.getVertexLabel() + ", Thursday, " + windowOfT4);

    assertTrue(propertiesV1ToCompare.contains("\"sum_Relevance\":2") &&
      propertiesV1ToCompare.contains("\"avg_Size\":15"));
    assertTrue(propertiesV2ToCompare.contains("\"sum_Relevance\":6") &&
      propertiesV2ToCompare.contains("\"avg_Size\":15"));
    assertTrue(propertiesV3ToCompare.contains("\"sum_Relevance\":4") &&
      propertiesV3ToCompare.contains("\"avg_Size\":30"));
    assertTrue(propertiesV4ToCompare.contains("\"sum_Relevance\":10") &&
      propertiesV4ToCompare.contains("\"avg_Size\":5"));
  }

@Test
public void testBuildVertexGroupProjectExpressions(){
  Expression[] toCompare = graphStreamGrouping.buildVertexGroupProjectExpressions();
  Object[] toOutput = Arrays.stream(toCompare).toArray();
  for (Object o : toOutput) {
    System.out.println(((ApiExpression )o).asSummaryString());
  }

  /*
  vertex_id
vertex_event_time
vertex_label
as(ExtractPropertyValue0(vertex_properties), 'TMP_10')
as(ExtractPropertyValue1(vertex_properties), 'TMP_11')
as(ExtractPropertyValue2(vertex_properties), 'TMP_12')
   */
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
  public void testPerformGroupingMethod() {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //Difference between timestamps: 10 seconds
    Timestamp t1 = new Timestamp(1619511661000L);
    Timestamp t2 = new Timestamp(1619511672000L);
    Timestamp t3 = new Timestamp(1619511683000L);
    Timestamp t4 = new Timestamp(1619511694000L);
    StreamVertex v1 = new StreamVertex("v1", "A", Properties.create(), t1);
    StreamVertex v2 = new StreamVertex("v2", "B", Properties.create(), t1);
    StreamVertex v3 = new StreamVertex("v3", "A", Properties.create(), t1);
    StreamVertex v4 = new StreamVertex("v4", "B", Properties.create(), t1);

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
    StreamTriple edge2 = new StreamTriple("2", t1, "impacts", propertiesE2, v3, v4);

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
    streamGraph.printVertices();

  }

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
}