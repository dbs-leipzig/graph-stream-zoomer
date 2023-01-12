package edu.dbsleipzig.stream.grouping.impl.algorithm;

import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.AvgProperty;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.MinProperty;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.SumProperty;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import edu.dbsleipzig.stream.grouping.model.table.TableSet;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
  TableGroupingBase.GroupingBuilder groupingBuilder;
  GraphStreamGrouping graphStreamGrouping;
  StreamTableEnvironment streamTableEnvironment;
  Table preparedVertices, furtherPreparedVertices, groupedVertices, newVertices, expandedVertices,
          edgesWithSuperVertices, enrichedEdges, groupedEdges, newEdges;
  Timestamp TsTot1, TsTot2, TsTot3, TsTot4,  windowOfT1, windowOfT2, windowOfT3, windowOfT4;
  final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  HashMap<String, String> vertexIdWithSuperVertexId;

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

    edge1 = new StreamTriple("e1", t2, "impacts",  propertiesE1, v1, v2);
    edge2 = new StreamTriple("e2", t4, "impacts", propertiesE2, v3, v4);
    edge3 = new StreamTriple("e3", t4, "calculates", propertiesE3, v3, v4);
    edge4 = new StreamTriple("e4", t2, "impacts",  propertiesE1, v1, v2);
    edge5 = new StreamTriple("e5", t2, "impacts", propertiesE2, v5, v6);
    edge6 = new StreamTriple("e6", t2, "calculates", propertiesE3, v5, v6);
    edge7 = new StreamTriple("e7", t4, "impacts",  propertiesE1, v7, v8);
    edge8 = new StreamTriple("e8", t4, "impacts", propertiesE2, v7, v8);
    edge9 = new StreamTriple("e9", t4, "calculates", propertiesE3, v7, v8);

    TsTot1 = new Timestamp(t1.getTime());
    TsTot2 = new Timestamp(t2.getTime());
    TsTot3 = new Timestamp(t3.getTime());
    TsTot4 = new Timestamp(t4.getTime());

    TsTot1.setSeconds((TsTot1.getSeconds()/10+1) * 10);
    TsTot2.setSeconds((TsTot2.getSeconds()/10+1) * 10);
    TsTot3.setSeconds((TsTot3.getSeconds()/10+1) * 10);
    TsTot4.setSeconds((TsTot4.getSeconds()/10+1) * 10);

    windowOfT1 = new Timestamp(TsTot1.getTime() - 1);
    windowOfT2 = new Timestamp(TsTot2.getTime() - 1);
    windowOfT3 = new Timestamp(TsTot3.getTime() - 1);
    windowOfT4 = new Timestamp(TsTot4.getTime() - 1);

    testStream = env.fromElements(edge1, edge2, edge3, edge4, edge5, edge6, edge7, edge8, edge9);
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

    graphStreamGrouping.setConfig(streamGraph.getConfig());
    graphStreamGrouping.setTableSet(streamGraph.getTableSet());
    streamTableEnvironment = graphStreamGrouping.getTableEnv();

    preparedVertices = graphStreamGrouping.prepareVertices();
    furtherPreparedVertices =
      convertLegacyToRawType(
        graphStreamGrouping.prepareVerticesFurther(preparedVertices));
    groupedVertices =
        graphStreamGrouping.groupVertices(furtherPreparedVertices);
    newVertices =
      graphStreamGrouping.createNewVertices(groupedVertices);
    expandedVertices =
            graphStreamGrouping.createExpandedVertices(furtherPreparedVertices, groupedVertices);
    edgesWithSuperVertices = graphStreamGrouping.createEdgesWithExpandedVertices(edges, expandedVertices);

    DataStream<Row> expandedVerticesRowStream = streamTableEnvironment.toDataStream(expandedVertices);
    ArrayList<HashMap<String, String>> resultingMaps = new ArrayList<>();
    vertexIdWithSuperVertexId = new HashMap<>();

    try {
      resultingMaps =
              (ArrayList<HashMap<String, String>>) expandedVerticesRowStream
                      .map(new GetSuperVertexIdOfVertexId()).executeAndCollect(10);
    } catch(Exception e) {e.printStackTrace();}

    for (HashMap<String, String> map : resultingMaps) {
      for (String key : map.keySet()) {
        vertexIdWithSuperVertexId.put(key, map.get(key));
      }
    }

    enrichedEdges = graphStreamGrouping.enrichEdgesWithSuperVertices(edgesWithSuperVertices);
    groupedEdges = graphStreamGrouping.groupEdges(enrichedEdges);
    newEdges = graphStreamGrouping.createNewEdges(groupedEdges);
  }

 class StreamToHashMapPreparedVertices implements  MapFunction<Row, HashMap<String, String>>  {
    public HashMap<String,String> map(Row row){
      HashMap<String, String> toReturn = new HashMap<>();
      Properties vertexPropertyContent = (Properties) row.getField("vertex_properties");
      String key = row.getField("vertex_id").toString();
      String value = "";
      value += "label: "+ row.getField("vertex_label");
      for (Property p : vertexPropertyContent) {
        value += ", " + p.getKey() + ": " +  p.getValue();
      }
      value += ", vertex_event_time: " +row.getField("vertex_event_time").toString().replaceAll("T", " ");
      toReturn.put(key, value);
      return toReturn;
    }
  }

  class StreamToHashMapFurtherPreparedVertices implements  MapFunction<Row, HashMap<String, String>> {
    public HashMap<String, String> map(Row row){
      //TMP_0 -> Weekday, TMP_1 -> Relevance, TMP_2 -> Size
      HashMap<String, String> toReturn = new HashMap<>();
      String key = row.getField("vertex_id").toString();
      String value = "label: " + row.getField("vertex_label");
      value += ", vertex_event_time: " + row.getField("vertex_event_time")
        .toString().replaceAll("T", " ");
      value += ", Weekday: " + row.getField("TMP_0");
      value += ", Relevance: " + row.getField("TMP_1");
      value += ", Size: " + row.getField("TMP_2");
      toReturn.put(key, value);
      return toReturn;
    }
  }

  class StreamToHashMapGroupedVertices implements  MapFunction<Row, HashMap<String, String>> {
    public HashMap<String, String> map(Row row){
      HashMap<String, String> toReturn = new HashMap<>();
      String key = row.getField("super_vertex_label").toString();
      key += ", " + row.getField("TMP_3");
      key += ", " + row.getField("super_vertex_rowtime").toString().replaceAll("T", " ");

      String value = "sum_Relevance: " + row.getField("TMP_4");
      value += ", avg_Size: " + row.getField("TMP_5");
      toReturn.put(key, value);
      return  toReturn;
    }
  }

  class StreamToHashMapNewVertices implements  MapFunction<Row, HashMap<String, String>>  {
    List<String> localVertexGroupingPropertyKeys;
    boolean localUseVertexLabel;
    StreamToHashMapNewVertices(boolean localUseVertexLabel, List<String> localVertexGroupingPropertyKeys){
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
      key += ", " +  row.getField("event_time").toString().replaceAll("T", " ");

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
  }

  class StreamToHashMapExpandedVertices implements  MapFunction<Row, HashMap<String, ArrayList<String>>> {
    public HashMap<String, ArrayList<String>> map(Row row){
      HashMap<String, ArrayList<String>> toReturn = new HashMap<>();
      String key = row.getField(TableSet.FIELD_VERTEX_ID).toString();
      String super_vertex_id = row.getField("super_vertex_id").toString();
      String event_time = row.getField("event_time").toString().replaceAll("T", " ");
      String super_vertex_label = row.getField("super_vertex_label").toString();
      ArrayList<String> value = new ArrayList<>();
      value.add(super_vertex_id);
      value.add(event_time);
      value.add(super_vertex_label);

      toReturn.put(key, value);
      return  toReturn;
    }
  }

  class GetSuperVertexIdOfVertexId implements MapFunction<Row, HashMap<String, String>> {
    public HashMap<String, String> map (Row row) {
      HashMap<String, String> toReturn = new HashMap<>();
      String key = row.getField(TableSet.FIELD_VERTEX_ID).toString();
      String value = row.getField("super_vertex_id").toString();
      toReturn.put(key, value);
      return toReturn;
    }
  }

  class StreamToHashMapEdgesWithSuperVertices implements MapFunction<Row, HashMap<String, ArrayList<String>>> {
    public HashMap<String, ArrayList<String>> map (Row row) {
      HashMap<String, ArrayList<String>> toReturn = new HashMap<>();
      String key = row.getField("edge_id").toString();
      String source_id = row.getField("source_id").toString();
      String target_id = row.getField("target_id").toString();
      ArrayList<String> value = new ArrayList<>();
      value.add(source_id);
      value.add(target_id);
      toReturn.put(key, value);
      return toReturn;
    }
  }

  class StreamToHashMapEnrichedEdges implements  MapFunction<Row, HashMap<String, ArrayList<String>>> {
    public HashMap<String, ArrayList<String>> map(Row row) {
      HashMap<String, ArrayList<String>> toReturn = new HashMap<>();
      String key = row.getField("edge_id").toString();
      String label = row.getField("edge_label").toString();
      String weekday = row.getField(5).toString();
      String weight = row.getField(6).toString();
      ArrayList<String> value = new ArrayList<>();
      value.add(label);
      value.add(weekday);
      value.add(weight);
      toReturn.put(key, value);
      return toReturn;
    }
  }

  class StreamToHashMapGroupedEdges implements  MapFunction<Row, HashMap<String, String>> {
    public HashMap<String, String> map(Row row){
      //TMP_8 -> Weekday, TMP_9 -> MinValue(Weight)
      HashMap<String, String> toReturn = new HashMap<>();
      String key = row.getField("edge_label").toString();
      key += ", " + row.getField(4);
      key += ", " + row.getField("event_time").toString().replaceAll("T", " ");
      String value = row.getField(5).toString();

      toReturn.put(key, value);
      return toReturn;
    }
  }

  class StreamToHashMapNewEdges implements  MapFunction<Row, HashMap<String, String>>  {
    public HashMap<String,String> map(Row row){
      HashMap<String, String> toReturn = new HashMap<>();
      String key = row.getField("edge_label").toString();
      Properties vertexPropertyContent = (Properties) row.getField("edge_properties");
      key += ", " + vertexPropertyContent.get("\"Weekday\"");
      key += ", " + row.getField("event_time").toString().replaceAll("T", " ");

      String value = vertexPropertyContent.get("\"min_Weight\"").toString();

      toReturn.put(key, value);
      return toReturn;
    }
  }


  @Test
  public void testPrepareVerticesMethod() throws Exception{
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(preparedVertices);
    HashMap<String, String> hashMapVertices = new HashMap<>();
    List<HashMap<String,String>> resultingMaps =
      rowStream.map(new StreamToHashMapPreparedVertices()).executeAndCollect(10);
    for (HashMap<String, String> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapVertices.put(key, result.get(key));
      }
    }

    String propertiesV1ToCompare = hashMapVertices.get(v1.getVertexId());
    String propertiesV2ToCompare = hashMapVertices.get(v2.getVertexId());
    String propertiesV3ToCompare = hashMapVertices.get(v3.getVertexId());
    String propertiesV4ToCompare = hashMapVertices.get(v4.getVertexId());
    String propertiesV5ToCompare = hashMapVertices.get(v5.getVertexId());
    String propertiesV6ToCompare = hashMapVertices.get(v6.getVertexId());
    String propertiesV7ToCompare = hashMapVertices.get(v7.getVertexId());
    String propertiesV8ToCompare = hashMapVertices.get(v8.getVertexId());


    assertTrue(propertiesV1ToCompare.contains("Relevance: 1") &&
      propertiesV1ToCompare.contains("Weekday: Monday") && propertiesV1ToCompare.contains("Size: 15")
      && propertiesV1ToCompare.contains("label: " + v1.getVertexLabel())
      && propertiesV1ToCompare.contains("vertex_event_time: "+windowOfT1));
    assertTrue(propertiesV2ToCompare.contains("Relevance: 3") &&
      propertiesV2ToCompare.contains("Weekday: Tuesday") && propertiesV2ToCompare.contains("Size: 15")
      && propertiesV2ToCompare.contains("label: " + v2.getVertexLabel())
      && propertiesV2ToCompare.contains("vertex_event_time: "+windowOfT2));;
    assertTrue(propertiesV3ToCompare.contains("Relevance: 2") &&
      propertiesV3ToCompare.contains("Weekday: Monday") && propertiesV3ToCompare.contains("Size: 30")
      && propertiesV3ToCompare.contains("label: " + v3.getVertexLabel())
      && propertiesV3ToCompare.contains("vertex_event_time: "+windowOfT3));
    assertTrue(propertiesV4ToCompare.contains("Relevance: 5") &&
      propertiesV4ToCompare.contains("Weekday: Thursday") && propertiesV4ToCompare.contains("Size: 5")
      && propertiesV4ToCompare.contains("label: " + v4.getVertexLabel())
      && propertiesV4ToCompare.contains("vertex_event_time: "+windowOfT4));
    assertTrue(propertiesV5ToCompare.contains("Relevance: 1") &&
      propertiesV5ToCompare.contains("Weekday: Monday") && propertiesV5ToCompare.contains("Size: 15")
      && propertiesV5ToCompare.contains("label: " + v5.getVertexLabel())
      && propertiesV5ToCompare.contains("vertex_event_time: "+windowOfT1));
    assertTrue(propertiesV6ToCompare.contains("Relevance: 3") &&
      propertiesV6ToCompare.contains("Weekday: Tuesday") && propertiesV6ToCompare.contains("Size: 15")
      && propertiesV6ToCompare.contains("label: " + v6.getVertexLabel())
      && propertiesV6ToCompare.contains("vertex_event_time: "+windowOfT2));
    assertTrue(propertiesV7ToCompare.contains("Relevance: 2") &&
      propertiesV7ToCompare.contains("Weekday: Monday") && propertiesV7ToCompare.contains("Size: 30")
      && propertiesV7ToCompare.contains("label: " + v7.getVertexLabel())
      && propertiesV7ToCompare.contains("vertex_event_time: "+windowOfT3));
    assertTrue(propertiesV8ToCompare.contains("Relevance: 5") &&
      propertiesV8ToCompare.contains("Weekday: Thursday") && propertiesV8ToCompare.contains("Size: 5")
      && propertiesV8ToCompare.contains("label: " + v8.getVertexLabel())
      && propertiesV8ToCompare.contains("vertex_event_time: "+windowOfT4));
  }

  @Test
  public void testFurtherPrepareVerticesMethod() throws Exception{
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(furtherPreparedVertices);
    HashMap<String, String> hashMapVertices = new HashMap<>();
    List<HashMap<String,String>> resultingMaps =
      rowStream.map(new StreamToHashMapFurtherPreparedVertices()).executeAndCollect(10);
    for (HashMap<String, String> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapVertices.put(key, result.get(key));
      }
    }

    String propertiesV1ToCompare = hashMapVertices.get(v1.getVertexId());
    String propertiesV2ToCompare = hashMapVertices.get(v2.getVertexId());
    String propertiesV3ToCompare = hashMapVertices.get(v3.getVertexId());
    String propertiesV4ToCompare = hashMapVertices.get(v4.getVertexId());
    String propertiesV5ToCompare = hashMapVertices.get(v5.getVertexId());
    String propertiesV6ToCompare = hashMapVertices.get(v6.getVertexId());
    String propertiesV7ToCompare = hashMapVertices.get(v7.getVertexId());
    String propertiesV8ToCompare = hashMapVertices.get(v8.getVertexId());

    assertTrue(propertiesV1ToCompare.contains("Relevance: 1") &&
      propertiesV1ToCompare.contains("Weekday: Monday") && propertiesV1ToCompare.contains("Size: 15")
      && propertiesV1ToCompare.contains("label: " + v1.getVertexLabel())
      && propertiesV1ToCompare.contains("vertex_event_time: "+windowOfT1));
    assertTrue(propertiesV2ToCompare.contains("Relevance: 3") &&
      propertiesV2ToCompare.contains("Weekday: Tuesday") && propertiesV2ToCompare.contains("Size: 15")
      && propertiesV2ToCompare.contains("label: " + v2.getVertexLabel())
      && propertiesV2ToCompare.contains("vertex_event_time: "+windowOfT2));;
    assertTrue(propertiesV3ToCompare.contains("Relevance: 2") &&
      propertiesV3ToCompare.contains("Weekday: Monday") && propertiesV3ToCompare.contains("Size: 30")
      && propertiesV3ToCompare.contains("label: " + v3.getVertexLabel())
      && propertiesV3ToCompare.contains("vertex_event_time: "+windowOfT3));
    assertTrue(propertiesV4ToCompare.contains("Relevance: 5") &&
      propertiesV4ToCompare.contains("Weekday: Thursday") && propertiesV4ToCompare.contains("Size: 5")
      && propertiesV4ToCompare.contains("label: " + v4.getVertexLabel())
      && propertiesV4ToCompare.contains("vertex_event_time: "+windowOfT4));
    assertTrue(propertiesV5ToCompare.contains("Relevance: 1") &&
      propertiesV5ToCompare.contains("Weekday: Monday") && propertiesV5ToCompare.contains("Size: 15")
      && propertiesV5ToCompare.contains("label: " + v5.getVertexLabel())
      && propertiesV5ToCompare.contains("vertex_event_time: "+windowOfT1));
    assertTrue(propertiesV6ToCompare.contains("Relevance: 3") &&
      propertiesV6ToCompare.contains("Weekday: Tuesday") && propertiesV6ToCompare.contains("Size: 15")
      && propertiesV6ToCompare.contains("label: " + v6.getVertexLabel())
      && propertiesV6ToCompare.contains("vertex_event_time: "+windowOfT2));
    assertTrue(propertiesV7ToCompare.contains("Relevance: 2") &&
      propertiesV7ToCompare.contains("Weekday: Monday") && propertiesV7ToCompare.contains("Size: 30")
      && propertiesV7ToCompare.contains("label: " + v7.getVertexLabel())
      && propertiesV7ToCompare.contains("vertex_event_time: "+windowOfT3));
    assertTrue(propertiesV8ToCompare.contains("Relevance: 5") &&
      propertiesV8ToCompare.contains("Weekday: Thursday") && propertiesV8ToCompare.contains("Size: 5")
      && propertiesV8ToCompare.contains("label: " + v8.getVertexLabel())
      && propertiesV8ToCompare.contains("vertex_event_time: "+windowOfT4));
  }

  @Test
  public void testGroupVerticesMethod() throws Exception{
    groupedVertices = convertLegacyToRawType(groupedVertices);
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(groupedVertices);
    List<HashMap<String,String>> resultingMaps =
      rowStream.map(new StreamToHashMapGroupedVertices()).executeAndCollect(10);

    HashMap<String, String> hashMapVertices = new HashMap<>();
    for (HashMap<String, String> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapVertices.put(key, result.get(key));
      }
    }

    String propertiesV1ToCompare = hashMapVertices.get("A, Monday, " + windowOfT1);
    String propertiesV2ToCompare = hashMapVertices.get("B, Tuesday, "+ windowOfT2);
    String propertiesV3ToCompare = hashMapVertices.get("A, Monday, " + windowOfT3);
    String propertiesV4ToCompare = hashMapVertices.get("B, Thursday, " + windowOfT4);

    assertTrue(propertiesV1ToCompare.contains("sum_Relevance: 2") &&
      propertiesV1ToCompare.contains("avg_Size: 15"));
    assertTrue(propertiesV2ToCompare.contains("sum_Relevance: 6") &&
      propertiesV2ToCompare.contains("avg_Size: 15"));
    assertTrue(propertiesV3ToCompare.contains("sum_Relevance: 4") &&
      propertiesV3ToCompare.contains("avg_Size: 30"));
    assertTrue(propertiesV4ToCompare.contains("sum_Relevance: 10") &&
      propertiesV4ToCompare.contains("avg_Size: 5"));
  }

  @Test
  public void testCreateNewVerticesMethod() throws Exception{
    newVertices = convertLegacyToRawType(newVertices);
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(newVertices);
    List<HashMap<String,String>> resultingMaps =
      rowStream.map(new StreamToHashMapNewVertices(graphStreamGrouping.useVertexLabels,
        graphStreamGrouping.vertexGroupingPropertyKeys)).executeAndCollect(10);

    HashMap<String, String> hashMapVertices = new HashMap<>();
    for (HashMap<String, String> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapVertices.put(key, result.get(key));
      }
    }

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
  public void testExpandedVerticesMethod() throws Exception {
    expandedVertices = convertLegacyToRawType(expandedVertices);
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(expandedVertices);
    List<HashMap<String,ArrayList<String>>> resultingMaps =
            rowStream.map(new StreamToHashMapExpandedVertices()).executeAndCollect(10);

    HashMap<String,ArrayList<String>> hashMapVertices = new HashMap<>();
    for (HashMap<String,ArrayList<String>> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapVertices.put(key, result.get(key));
      }
    }

    //super_vertex_id || event_time || super_vertex_label
    ArrayList<String> propertiesV1 = hashMapVertices.get("v1");
    ArrayList<String> propertiesV2 = hashMapVertices.get("v2");
    ArrayList<String> propertiesV3 = hashMapVertices.get("v3");
    ArrayList<String> propertiesV4 = hashMapVertices.get("v4");
    ArrayList<String> propertiesV5 = hashMapVertices.get("v5");
    ArrayList<String> propertiesV6 = hashMapVertices.get("v6");
    ArrayList<String> propertiesV7 = hashMapVertices.get("v7");
    ArrayList<String> propertiesV8 = hashMapVertices.get("v8");


    assertTrue( propertiesV1.get(1).equals(windowOfT1.toString()) && propertiesV1.get(2).equals("A") &&
            propertiesV1.get(0).equals(propertiesV5.get(0)));
    assertTrue(propertiesV2.get(1).equals(windowOfT2.toString()) && propertiesV2.get(2).equals("B") &&
            propertiesV2.get(0).equals(propertiesV6.get(0)));
    assertTrue(propertiesV3.get(1).equals(windowOfT3.toString()) && propertiesV3.get(2).equals("A") &&
            propertiesV3.get(0).equals(propertiesV7.get(0)));
    assertTrue(propertiesV4.get(1).equals(windowOfT4.toString()) && propertiesV4.get(2).equals("B") &&
            propertiesV4.get(0).equals(propertiesV8.get(0)));
    assertTrue(propertiesV5.get(1).equals(windowOfT1.toString()) && propertiesV5.get(2).equals("A") &&
            propertiesV5.get(0).equals(propertiesV1.get(0)));
    assertTrue(propertiesV6.get(1).equals(windowOfT2.toString()) && propertiesV6.get(2).equals("B") &&
            propertiesV6.get(0).equals(propertiesV2.get(0)));
    assertTrue(propertiesV7.get(1).equals(windowOfT3.toString()) && propertiesV7.get(2).equals("A") &&
            propertiesV7.get(0).equals(propertiesV3.get(0)));
    assertTrue(propertiesV8.get(1).equals(windowOfT4.toString()) && propertiesV8.get(2).equals("B") &&
            propertiesV8.get(0).equals(propertiesV4.get(0)));
  }

  @Test
  public void testEdgesWithSuperVerticesMethod() throws Exception {
    edgesWithSuperVertices = convertLegacyToRawType(edgesWithSuperVertices);
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(edgesWithSuperVertices);
    List<HashMap<String,ArrayList<String>>> resultingMaps =
            rowStream.map(new StreamToHashMapEdgesWithSuperVertices()).executeAndCollect(10);

    HashMap<String,ArrayList<String>> hashMapEdges = new HashMap<>();
    for (HashMap<String,ArrayList<String>> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapEdges.put(key, result.get(key));
      }
    }

    ArrayList<String> verticesE1 = hashMapEdges.get("e1");
    ArrayList<String> verticesE2 = hashMapEdges.get("e2");
    ArrayList<String> verticesE3 = hashMapEdges.get("e3");
    ArrayList<String> verticesE4 = hashMapEdges.get("e4");
    ArrayList<String> verticesE5 = hashMapEdges.get("e5");
    ArrayList<String> verticesE6 = hashMapEdges.get("e6");
    ArrayList<String> verticesE7 = hashMapEdges.get("e7");
    ArrayList<String> verticesE8 = hashMapEdges.get("e8");
    ArrayList<String> verticesE9 = hashMapEdges.get("e9");

    assertTrue(verticesE1.get(1).equals(vertexIdWithSuperVertexId.get("v1"))
            && verticesE1.get(0).equals(vertexIdWithSuperVertexId.get("v2")));
    assertTrue(verticesE2.get(1).equals(vertexIdWithSuperVertexId.get("v3"))
            && verticesE2.get(0).equals(vertexIdWithSuperVertexId.get("v4")));
    assertTrue(verticesE3.get(1).equals(vertexIdWithSuperVertexId.get("v3"))
            && verticesE3.get(0).equals(vertexIdWithSuperVertexId.get("v4")));
    assertTrue(verticesE4.get(1).equals(vertexIdWithSuperVertexId.get("v1"))
            && verticesE4.get(0).equals(vertexIdWithSuperVertexId.get("v2")));
    assertTrue(verticesE5.get(1).equals(vertexIdWithSuperVertexId.get("v5"))
            && verticesE5.get(0).equals(vertexIdWithSuperVertexId.get("v6")));
    assertTrue(verticesE6.get(1).equals(vertexIdWithSuperVertexId.get("v5"))
            && verticesE6.get(0).equals(vertexIdWithSuperVertexId.get("v6")));
    assertTrue(verticesE7.get(1).equals(vertexIdWithSuperVertexId.get("v7"))
            && verticesE7.get(0).equals(vertexIdWithSuperVertexId.get("v8")));
    assertTrue(verticesE8.get(1).equals(vertexIdWithSuperVertexId.get("v7"))
            && verticesE8.get(0).equals(vertexIdWithSuperVertexId.get("v8")));
    assertTrue(verticesE9.get(1).equals(vertexIdWithSuperVertexId.get("v7"))
            && verticesE9.get(0).equals(vertexIdWithSuperVertexId.get("v8")));
  }

  @Test
  public void testEnrichEdgesMethod() throws Exception {
    enrichedEdges = convertLegacyToRawType(enrichedEdges);
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(enrichedEdges);
    List<HashMap<String,ArrayList<String>>> resultingMaps =
            rowStream.map(new StreamToHashMapEnrichedEdges()).executeAndCollect(10);

    HashMap<String,ArrayList<String>> hashMapEdges = new HashMap<>();
    for (HashMap<String,ArrayList<String>> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapEdges.put(key, result.get(key));
      }
    }

    ArrayList<String> propertiesE1 = hashMapEdges.get("e1");
    ArrayList<String> propertiesE2 = hashMapEdges.get("e2");
    ArrayList<String> propertiesE3 = hashMapEdges.get("e3");
    ArrayList<String> propertiesE4 = hashMapEdges.get("e4");
    ArrayList<String> propertiesE5 = hashMapEdges.get("e5");
    ArrayList<String> propertiesE6 = hashMapEdges.get("e6");
    ArrayList<String> propertiesE7 = hashMapEdges.get("e7");
    ArrayList<String> propertiesE8 = hashMapEdges.get("e8");
    ArrayList<String> propertiesE9 = hashMapEdges.get("e9");

    assertTrue(propertiesE1.get(0).equals("impacts") && propertiesE1.get(1).equals("Thursday") &&
            propertiesE1.get(2).equals("5"));
    assertTrue(propertiesE2.get(0).equals("impacts") && propertiesE2.get(1).equals("Wednesday") &&
            propertiesE2.get(2).equals("6"));
    assertTrue(propertiesE3.get(0).equals("calculates") && propertiesE3.get(1).equals("Thursday") &&
            propertiesE3.get(2).equals("3"));
    assertTrue(propertiesE4.get(0).equals("impacts") && propertiesE4.get(1).equals("Thursday") &&
            propertiesE4.get(2).equals("5"));
    assertTrue(propertiesE5.get(0).equals("impacts") && propertiesE5.get(1).equals("Wednesday") &&
            propertiesE5.get(2).equals("6"));
    assertTrue(propertiesE6.get(0).equals("calculates") && propertiesE6.get(1).equals("Thursday") &&
            propertiesE6.get(2).equals("3"));
    assertTrue(propertiesE7.get(0).equals("impacts") && propertiesE7.get(1).equals("Thursday") &&
            propertiesE7.get(2).equals("5"));
    assertTrue(propertiesE8.get(0).equals("impacts") && propertiesE8.get(1).equals("Wednesday") &&
            propertiesE8.get(2).equals("6"));
    assertTrue(propertiesE9.get(0).equals("calculates") && propertiesE9.get(1).equals("Thursday") &&
            propertiesE9.get(2).equals("3"));
  }

  @Test
  public void testEdgeGroupingMethod() throws Exception {
    groupedEdges = convertLegacyToRawType(groupedEdges);
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(groupedEdges);
    HashMap<String, String> hashMapGroupedEdges = new HashMap<>();
    List<HashMap<String,String>> resultingMaps =
            rowStream.map(new StreamToHashMapGroupedEdges()).executeAndCollect(10);
    for (HashMap<String, String> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapGroupedEdges.put(key, result.get(key));
      }
    }

    String propertyGroupedEdge1 = hashMapGroupedEdges.get("impacts, Thursday, " + windowOfT2);
    String propertyGroupedEdge2 = hashMapGroupedEdges.get("impacts, Wednesday, " + windowOfT1);
    String propertyGroupedEdge3 = hashMapGroupedEdges.get("calculates, Thursday, "+ windowOfT1);
    String propertyGroupedEdge4 = hashMapGroupedEdges.get("calculates, Thursday, " + windowOfT2);
    String propertyGroupedEdge5 = hashMapGroupedEdges.get("impacts, Thursday, " + windowOfT1);
    String propertyGroupedEdge6 = hashMapGroupedEdges.get("impacts, Wednesday, " + windowOfT2);

    assertTrue(propertyGroupedEdge1.equals("5"));
    assertTrue(propertyGroupedEdge2.equals("6"));
    assertTrue(propertyGroupedEdge3.equals("3"));
    assertTrue(propertyGroupedEdge4.equals("3"));
    assertTrue(propertyGroupedEdge5.equals("5"));
    assertTrue(propertyGroupedEdge6.equals("6"));
  }

  @Test
  public void testCreateNewEdgesMethod() throws Exception{
    newEdges = convertLegacyToRawType(newEdges);
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(newEdges);
    List<HashMap<String,String>> resultingMaps =
            rowStream.map(new StreamToHashMapNewEdges()).executeAndCollect(10);

    HashMap<String, String> hashMapNewEdges = new HashMap<>();
    for (HashMap<String, String> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapNewEdges.put(key, result.get(key));
      }
    }

    String propertyGroupedEdge1 = hashMapNewEdges.get("impacts, Thursday, " + windowOfT2);
    String propertyGroupedEdge2 = hashMapNewEdges.get("impacts, Wednesday, " + windowOfT1);
    String propertyGroupedEdge3 = hashMapNewEdges.get("calculates, Thursday, "+ windowOfT1);
    String propertyGroupedEdge4 = hashMapNewEdges.get("calculates, Thursday, " + windowOfT2);
    String propertyGroupedEdge5 = hashMapNewEdges.get("impacts, Thursday, " + windowOfT1);
    String propertyGroupedEdge6 = hashMapNewEdges.get("impacts, Wednesday, " + windowOfT2);

    assertTrue(propertyGroupedEdge1.equals("5"));
    assertTrue(propertyGroupedEdge2.equals("6"));
    assertTrue(propertyGroupedEdge3.equals("3"));
    assertTrue(propertyGroupedEdge4.equals("3"));
    assertTrue(propertyGroupedEdge5.equals("5"));
    assertTrue(propertyGroupedEdge6.equals("6"));

  }

  public Table convertLegacyToRawType(Table table) {
    return streamTableEnvironment.sqlQuery("SELECT * FROM " +table);
  }

}