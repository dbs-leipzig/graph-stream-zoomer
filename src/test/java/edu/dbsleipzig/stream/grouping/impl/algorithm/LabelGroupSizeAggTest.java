package edu.dbsleipzig.stream.grouping.impl.algorithm;

import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.SumProperty;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.WindowConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import static org.junit.Assert.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
   TODO:
        - Check properly set window-timestamps
        - Change hard-coded ground-truth-timestamps to external calculation based on base-vertex-timestamps
        - Check if unique super-Ids for vertices and edges (not separately) <-- currently buggy
        - Only check prepareVertices/newVertices-/newEdges-Method once?
 */
public class LabelGroupSizeAggTest {

    static StreamExecutionEnvironment env;
    static StreamTableEnvironment tEnv;
    static StreamGraph streamGraph;
    static GraphStreamGrouping graphStreamGrouping;
    static Table vertices, preparedVertices, furtherPreparedVertices, groupedVertices, newVertices, expandedVertices;
    static Table edges, edgesWithExpandedVertices, enrichedEdgesWithSuperVertices, groupedEdges, newEdges;
    //Necessary for mapping for enrichedEdges-test
    static HashMap<String, String> vId_vSupId = new HashMap<>();

    @BeforeClass
    public static void initialize() throws Exception {
         env = Initializer.getExecutionEnvironment();
         streamGraph = Initializer.getStreamGraph();

         List<CustomizedAggregationFunction> vertexAggregations = new ArrayList<>();
         vertexAggregations.add(new SumProperty("Size"));
         List<CustomizedAggregationFunction> edgeAggregations = new ArrayList<>();
         edgeAggregations.add(new SumProperty("Weight"));

         graphStreamGrouping = new GraphStreamGrouping(true, true, new ArrayList<>(),
                 vertexAggregations, new ArrayList<>(), edgeAggregations,
           WindowConfig.create().setValue(10).setUnit(WindowConfig.TimeUnit.SECONDS));
         graphStreamGrouping.setConfig(streamGraph.getConfig());
         graphStreamGrouping.setTableSet(streamGraph.getTableSet());
         tEnv = graphStreamGrouping.getTableEnv();

         vertices = streamGraph.getTableSet().getVertices();
         preparedVertices = graphStreamGrouping.deduplicateVertices();
         furtherPreparedVertices = graphStreamGrouping.enhanceVerticesByPropertyColumns(preparedVertices);
         groupedVertices = graphStreamGrouping.groupVertices(furtherPreparedVertices);
         newVertices = graphStreamGrouping.createSuperVertices(groupedVertices);
         expandedVertices = graphStreamGrouping.createExpandedVertices(furtherPreparedVertices, groupedVertices);

         edges = streamGraph.getTableSet().getEdges();
         edgesWithExpandedVertices = graphStreamGrouping.createEdgesWithExpandedVertices(edges, expandedVertices);
         enrichedEdgesWithSuperVertices = graphStreamGrouping.enrichEdgesWithSuperVertices(edgesWithExpandedVertices);
         groupedEdges = graphStreamGrouping.groupEdges(enrichedEdgesWithSuperVertices);
         newEdges = graphStreamGrouping.createSuperEdges(groupedEdges);

         //Mapping vertex -> superVertex is necessary in multiple test methods
         DataStream<Row> evRows = tEnv.toDataStream(expandedVertices);
         ArrayList<Row> evList = new ArrayList<>(evRows.executeAndCollect(20));
         for (Row r : evList) {
             String id = r.getField("vertex_id").toString();
             String supId = r.getField("super_vertex_id").toString();
                vId_vSupId.put(id, supId);
         }
    }

    /**
     * Checks for duplicates after deduplication based StreamVertex-Hashcode
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testPrepareVerticesDeduplication() throws Exception {
        ArrayList<StreamVertex> vertexList = new ArrayList<>(tEnv.toDataStream(preparedVertices, StreamVertex.class).executeAndCollect(20));
        ArrayList<StreamVertex> baseVertexList = new ArrayList<>(tEnv.toDataStream(vertices, StreamVertex.class).executeAndCollect(20));
        HashSet<Integer> hashCodes = new HashSet<>();
        for (StreamVertex v : baseVertexList) {
            hashCodes.add(v.hashCode());
        }
        assertEquals(vertexList.size(), hashCodes.size());
    }

    /**
     * Checks for column-extension based on the arity of the row
     * Checks for column-naming based on the field names of the row
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testFurtherPreparedVertices() throws Exception {
        DataStream<Row> fpRows = tEnv.toDataStream(furtherPreparedVertices);
        ArrayList<Row> fpList = new ArrayList<>(fpRows.executeAndCollect(10));
        Row r = fpList.get(0);
        Set<String> fieldNames = r.getFieldNames(true);
        assertEquals(r.getArity(), 4);
        assertTrue(fieldNames.contains("vertex_id"));
        assertTrue(fieldNames.contains("vertex_event_time"));
        assertTrue(fieldNames.contains("vertex_label"));
        assertTrue(fieldNames.contains("TMP_0"));
    }

    /**
     * Checks duplicates in assigned Super-Vertex-IDs
     * Checks if summation of size for corresponding label and window is correct
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testGroupedVertices() throws Exception {
        DataStream<Row> gvRows = tEnv.toDataStream(groupedVertices);
        ArrayList<Row> gvList = new ArrayList<>(gvRows.executeAndCollect(20));
        HashSet<String> svIds = new HashSet<>();
        for (Row r : gvList) {
            svIds.add((String) r.getField("super_vertex_id"));
            String event_time = r.getField("super_vertex_rowtime").toString();
            String label = (String) r.getField("super_vertex_label");
            int sumSize = Integer.parseInt(r.getField("TMP_1").toString());
            if (event_time.equals("2021-04-27 10:21:09.999") && label.equals("A")) {
                assertEquals(sumSize, 45);
            }
            else if (event_time.equals("2021-04-27 10:21:09.999") && label.equals("B")) {
                assertEquals(sumSize, 15);
            }
            else if (event_time.equals("2021-04-27 10:21:19.999") && label.equals("A")) {
                assertEquals(sumSize, 30);
            }
            else if (event_time.equals("2021-04-27 10:21:19.999") && label.equals("B")) {
                assertEquals(sumSize, 15);
            }
            else {
                throw new AssertionError("Unexpected timestamp or label after grouping. Timestamp: " + event_time + ", Label: " + label);
            }
        }
        assertEquals(svIds.size(), gvList.size());
    }

    /**
     * Checks if column vertex_properties is correctly created
     * Checks if vertex_properties-entries have the correct syntax
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testNewVertices() throws Exception {
        DataStream<Row> nvRows = tEnv.toDataStream(newVertices);
        ArrayList<Row> nvList = new ArrayList<>(nvRows.executeAndCollect(20));
        Pattern propertiesPattern = Pattern.compile("sum_Size=\\d+:Integer");
        for (Row r : nvList) {
            Set<String> fieldNames = r.getFieldNames(true);
            assertTrue(fieldNames.contains("vertex_properties"));
            assertEquals(r.getArity(), 4);

            String vertex_properties = r.getField("vertex_properties").toString();
            Matcher matcher = propertiesPattern.matcher(vertex_properties);
            assertTrue(matcher.matches());
        }
    }

    /**
     * Tests if vertices in the same group were assigned to the same super_vertex_id
     * Groups: [(v1,v3), (v2,v4), (v5,v7), (v6,v8)]
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testExpandedVertices() throws Exception {
        DataStream<Row> evRows = tEnv.toDataStream(expandedVertices);
        ArrayList<Row> evList = new ArrayList<>(evRows.executeAndCollect(20));

        for (Row r : evList) {
            String id = r.getField("vertex_id").toString();
            String supId = r.getField("super_vertex_id").toString();
            switch(id) {
                case "v3": assertEquals(vId_vSupId.get("v1"), supId); break;
                case "v4": assertEquals(vId_vSupId.get("v2"), supId); break;
                case "v7": assertEquals(vId_vSupId.get("v5"), supId); break;
                case "v8": assertEquals(vId_vSupId.get("v6"), supId); break;
            }
        }
    }

    /**
     * Checks if the edges are mapped on the correct super-vertex
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testEdgesWithExpandedVertices() throws Exception {
        DataStream<Row> eRow = tEnv.toDataStream(edges);
        DataStream<Row> eevRow = tEnv.toDataStream(edgesWithExpandedVertices);
        ArrayList<Row> eList = new ArrayList<>(eRow.executeAndCollect(20));
        ArrayList<Row> eevList = new ArrayList<>(eevRow.executeAndCollect(20));
        HashMap<String, Tuple2<String, String>> source_target = new HashMap<>();
        for (Row r : eList) {
            String edgeId = r.getField("edge_id").toString();
            String source = r.getField("source_id").toString();
            String target = r.getField("target_id").toString();
            source_target.put(edgeId, new Tuple2<>(source, target));
        }
        for (Row r : eevList) {
            String edgeId = r.getField("edge_id").toString();
            String superSource = r.getField("source_id").toString();
            String superTarget = r.getField("target_id").toString();
            String source = source_target.get(edgeId).f0;
            String target = source_target.get(edgeId).f1;
            assertEquals(superSource, vId_vSupId.get(source));
            assertEquals(superTarget, vId_vSupId.get(target));
        }
    }

    /**
     * Checks for column-extension based on the arity of the row
     * Checks for column-naming based on the field names of the row
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testEnrichedEdgesWithSuperVertices() throws Exception {
        DataStream<Row> eesRow = tEnv.toDataStream(enrichedEdgesWithSuperVertices);
        ArrayList<Row> eesList = new ArrayList<>(eesRow.executeAndCollect(20));
        Row r = eesList.get(0);
        Set<String> fieldNames = r.getFieldNames(true);
        assertEquals(r.getArity(), 6);
        assertTrue(fieldNames.contains("edge_id"));
        assertTrue(fieldNames.contains("event_time"));
        assertTrue(fieldNames.contains("source_id"));
        assertTrue(fieldNames.contains("target_id"));
        assertTrue(fieldNames.contains("edge_label"));
        assertTrue(fieldNames.contains("TMP_8"));
    }

    /**
     * Checks duplicates in assigned Super-Edge-IDs
     * Checks if summation of weight for corresponding label and window is correct
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testGroupedEdges() throws Exception {
        DataStream<Row> gRows = tEnv.toDataStream(groupedEdges);
        ArrayList<Row> gList = new ArrayList<>(gRows.executeAndCollect(20));
        HashSet<String> seIds = new HashSet<>();
        for (Row r : gList) {
            seIds.add(r.getField("super_edge_id").toString());
            String edgeLabel = r.getField("edge_label").toString();
            String event_time = r.getField("event_time").toString();
            int sumWeight = Integer.parseInt(r.getField("TMP_9").toString());
            if (event_time.equals("2021-04-27 10:21:09.999") && edgeLabel.equals("impacts")) {
                assertEquals(6, sumWeight);
            }
            else if (event_time.equals("2021-04-27 10:21:09.999") && edgeLabel.equals("calculates")) {
                assertEquals(3, sumWeight);
            }
            else if (event_time.equals("2021-04-27 10:21:19.999") && edgeLabel.equals("impacts")) {
                assertEquals(12, sumWeight);
            }
            else if (event_time.equals("2021-04-27 10:21:19.999") && edgeLabel.equals("calculates")) {
                assertEquals(3, sumWeight);
            }
            else {
                throw new AssertionError("Unexpected timestamp or label after grouping. Timestamp: " + event_time + ", Label: " + edgeLabel);
            }
        }
        assertEquals(gList.size(), seIds.size());
    }

    /**
     * Checks if column edge_properties is correctly created
     * Checks if edge_properties-entries have the correct syntax
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testNewEdges() throws Exception {
        DataStream<Row> neRows = tEnv.toDataStream(newEdges);
        ArrayList<Row> neList = new ArrayList<>(neRows.executeAndCollect(20));
        Pattern propertiesPattern = Pattern.compile("sum_Weight=\\d+:Integer");
        for (Row r : neList) {
            Set<String> fieldNames = r.getFieldNames(true);
            assertTrue(fieldNames.contains("edge_properties"));
            assertEquals(r.getArity(), 6);

            String vertex_properties = r.getField("edge_properties").toString();
            Matcher matcher = propertiesPattern.matcher(vertex_properties);
            assertTrue(matcher.matches());
        }
    }
}
