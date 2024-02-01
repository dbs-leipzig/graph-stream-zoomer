package edu.dbsleipzig.stream.grouping.impl.algorithm;

import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.Count;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.WindowConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FullGroupCountAggTest {

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
        vertexAggregations.add(new Count());
        List<CustomizedAggregationFunction> edgeAggregations = new ArrayList<>();
        edgeAggregations.add(new Count());

        graphStreamGrouping = new GraphStreamGrouping(false, false, new ArrayList<>(),
                vertexAggregations, new ArrayList<>(), edgeAggregations,
                WindowConfig.create().setValue(1).setUnit(WindowConfig.TimeUnit.MINUTES));
        graphStreamGrouping.setConfig(streamGraph);
        graphStreamGrouping.setTableSet(streamGraph);
        tEnv = graphStreamGrouping.getTableEnv();

        vertices = streamGraph.getTableSet().getVertices();
        preparedVertices = graphStreamGrouping.prepareVertices();
        furtherPreparedVertices = graphStreamGrouping.prepareVerticesFurther(preparedVertices);
        groupedVertices = graphStreamGrouping.groupVertices(furtherPreparedVertices);
        newVertices = graphStreamGrouping.createNewVertices(groupedVertices);
        expandedVertices = graphStreamGrouping.createExpandedVertices(furtherPreparedVertices, groupedVertices);

        edges = streamGraph.getTableSet().getEdges();
        edgesWithExpandedVertices = graphStreamGrouping.createEdgesWithExpandedVertices(edges, expandedVertices);
        enrichedEdgesWithSuperVertices = graphStreamGrouping.enrichEdgesWithSuperVertices(edgesWithExpandedVertices);
        groupedEdges = graphStreamGrouping.groupEdges(enrichedEdgesWithSuperVertices);
        newEdges = graphStreamGrouping.createNewEdges(groupedEdges);

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
     * Checks if the label is an empty string
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testFurtherPreparedVertices() throws Exception {
        DataStream<Row> fpRows = tEnv.toDataStream(furtherPreparedVertices);
        ArrayList<Row> fpList = new ArrayList<>(fpRows.executeAndCollect(10));
        Row r = fpList.get(0);
        Set<String> fieldNames = r.getFieldNames(true);
        assertEquals(r.getArity(), 3);
        assertTrue(fieldNames.contains("vertex_id"));
        assertTrue(fieldNames.contains("vertex_event_time"));
        assertTrue(fieldNames.contains("vertex_label"));
        assertEquals("", r.getField("vertex_label").toString());
    }

    /**
     * Checks if all vertices summed up into a single one
     * Checks if the count for corresponding window is correct
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testGroupedVertices() throws Exception {
        DataStream<Row> gRows = tEnv.toDataStream(groupedVertices);
        ArrayList<Row> gList = new ArrayList<>(gRows.executeAndCollect(20));
        assertEquals(1, gList.size());
        for (Row r : gList) {
            assertEquals("2021-04-27 10:21:59.999", r.getField("super_vertex_rowtime").toString());
            assertEquals("", r.getField("super_vertex_label"));
            assertEquals(8, Integer.parseInt(r.getField("TMP_0").toString()));
        }
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
        Pattern propertiesPattern = Pattern.compile("count=\\d+:Long");
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
     * Checks if all vertices were assigned to the same super-vertex
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testExpandedVertices() throws Exception {
        DataStream<Row> evRow = tEnv.toDataStream(expandedVertices);
        ArrayList<Row> evList = new ArrayList<>(evRow.executeAndCollect(20));
        String supId = vId_vSupId.get("v1");
        for (Row r : evList) {
            assertEquals(supId, r.getField("super_vertex_id").toString());
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
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testEnrichedEdgesWithSuperVertices() throws Exception {
        DataStream<Row> eesRow = tEnv.toDataStream(enrichedEdgesWithSuperVertices);
        ArrayList<Row> eesList = new ArrayList<>(eesRow.executeAndCollect(20));
        Row r = eesList.get(0);
        assertEquals(5, r.getArity());
    }

    /**
     * Checks if all edges summed up into a single one
     * Checks if the count for corresponding window is correct
     * @throws Exception if Table-DataStream-Conversion fails
     */
    @Test
    public void testGroupedEdges() throws Exception {
        DataStream<Row> gRows = tEnv.toDataStream(groupedEdges);
        ArrayList<Row> gList = new ArrayList<>(gRows.executeAndCollect(20));
        assertEquals(1, gList.size());
        for (Row r : gList) {
            assertEquals("2021-04-27 10:21:59.999", r.getField("event_time").toString());
            assertEquals("", r.getField("edge_label"));
            assertEquals(8, Integer.parseInt(r.getField("TMP_7").toString()));
        }
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
        Pattern propertiesPattern = Pattern.compile("count=\\d+:Long");
        for (Row r : neList) {
            Set<String> fieldNames = r.getFieldNames(true);
            assertTrue(fieldNames.contains("edge_properties"));
            assertEquals(6, r.getArity());

            String vertex_properties = r.getField("edge_properties").toString();
            Matcher matcher = propertiesPattern.matcher(vertex_properties);
            assertTrue(matcher.matches());
        }
    }
}
