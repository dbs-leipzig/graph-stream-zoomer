package edu.leipzig.application;

import edu.leipzig.application.functions.JSONToStreamObjectMapper;
import edu.leipzig.impl.algorithm.GraphStreamGrouping;
import edu.leipzig.impl.algorithm.TableGroupingBase;
import edu.leipzig.impl.functions.aggregation.*;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.graph.StreamTriple;
import edu.leipzig.model.graph.StreamVertex;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

public class GraphSummarizationJob {

    public static void main(String[] args) throws Exception {

        /*
        Hier noch definieren von Graphen ohne Userinput. Wie wird graph sonst eingegeben?
         */

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        propertiesVertexV1.put("Weekday", "Monday");
        Properties propertiesV1 = Properties.createFromMap(propertiesVertexV1);
        v1.setVertexProperties(propertiesV1);

        HashMap<String, Object> propertiesVertexV2 = new HashMap<>();
        propertiesVertexV2.put("Relevance", 3);
        propertiesVertexV2.put("Size", 10);
        propertiesVertexV2.put("Weekday", "Tuesday");
        Properties propertiesV2 = Properties.createFromMap(propertiesVertexV2);
        v2.setVertexProperties(propertiesV2);

        HashMap<String, Object> propertiesVertexV3 = new HashMap<>();
        propertiesVertexV3.put("Relevance", 2);
        propertiesVertexV3.put("Size", 30);
        propertiesVertexV3.put("Weekday", "Wednesday");
        Properties propertiesV3 = Properties.createFromMap(propertiesVertexV3);
        v3.setVertexProperties(propertiesV3);

        HashMap<String, Object> propertiesVertexV4 = new HashMap<>();
        propertiesVertexV4.put("Relevance", 5);
        propertiesVertexV4.put("Size", 5);
        propertiesVertexV4.put("Weekday", "Thursday");
        Properties propertiesV4 = Properties.createFromMap(propertiesVertexV4);
        v4.setVertexProperties(propertiesV4);

        HashMap<String, Object> propertiesEdge1 = new HashMap<>();
        propertiesEdge1.put("Weight", 5);
        propertiesEdge1.put("Weekday", "Thursday");
        Properties propertiesE1 = Properties.createFromMap(propertiesEdge1);

        HashMap<String, Object> propertiesEdge2 = new HashMap<>();
        propertiesEdge2.put("Weight", 6);
        propertiesEdge2.put("Weekday", "Wednesday");
        Properties propertiesE2 = Properties.createFromMap(propertiesEdge2);

        StreamTriple edge1 = new StreamTriple("1", t1, "impacts",  propertiesE1, v1, v2);
        StreamTriple edge2 = new StreamTriple("2", t1, "impacts", propertiesE2, v3, v4);
        StreamTriple edge3 = new StreamTriple("3", t1, "calculates", propertiesE1, v3, v4);

        DataStream<StreamTriple> testStream = env.fromElements(edge1, edge2, edge3);

        /*
        args: 1: windowsize
              2: vertexGroupingKeys
              3: vertexAggregateFunctions
              4: edgeGroupingKeys
              5: edgeAggregateFunctions
         */
        int windowSize = 0;
        String[] vertexGroupingKeys = null;
        String[] vertexAggregateFunctions = null;
        String[] edgeGroupingKeys = null;
        String[] edgeAggregateFunctions = null;
        if (args.length == 5) {
            windowSize = Integer.parseInt(args[0]);
            vertexGroupingKeys = args[1].replace(" ", "").split(",");
            vertexAggregateFunctions = args[2].replace(" ", "").split(",");
            edgeGroupingKeys = args[3].replace(" ", "").split(",");
            edgeAggregateFunctions = args[4].replace(" ", "").split(",");
        }
        else {
            System.out.println("Wrong amount of input parameters");
        }

        DataStream<StreamTriple> socketStream = env.socketTextStream("localhost", 6666)
                .map(new JSONToStreamObjectMapper()).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StreamTriple>forBoundedOutOfOrderness(Duration.ofSeconds(windowSize))
                                .withTimestampAssigner((event,timestamp) -> event.getTimestamp().getTime()));

        StreamGraph streamGraph = StreamGraph.fromFlinkStream(testStream, new StreamGraphConfig(env));

        TableGroupingBase.GroupingBuilder groupingBuilder = new TableGroupingBase.GroupingBuilder();
        for (String vertexGroupingKey : vertexGroupingKeys) {
            groupingBuilder.addVertexGroupingKey(vertexGroupingKey);
        }
        for (String edgeGroupingKey : edgeGroupingKeys) {
            groupingBuilder.addEdgeGroupingKey(edgeGroupingKey);
        }
        for (CustomizedAggregationFunction customizedAggregationFunction : inputToCustomizedAggFunction(vertexAggregateFunctions)) {
            groupingBuilder.addVertexAggregateFunction(customizedAggregationFunction);
        }
        for (CustomizedAggregationFunction customizedAggregationFunction : inputToCustomizedAggFunction(edgeAggregateFunctions)) {
            groupingBuilder.addEdgeAggregateFunction(customizedAggregationFunction);
        }

        streamGraph = groupingBuilder.build().execute(streamGraph);
        //streamGraph.print();


    }

    private static ArrayList<CustomizedAggregationFunction> inputToCustomizedAggFunction(String[] stringFunctions) {
        ArrayList<CustomizedAggregationFunction> customizedAggregationFunctions = new ArrayList<>();
        for (String s : stringFunctions) {
            String[] split = s.split("-");
            String argument = split[1];
            String function = split[0];
            if (function.trim().equalsIgnoreCase("minproperty")) {
                customizedAggregationFunctions.add(new MinProperty(argument.trim()));
            }
            if (function.trim().equalsIgnoreCase("avgProperty")) {
                customizedAggregationFunctions.add(new AvgProperty(argument.trim()));
            }
            if (function.trim().equalsIgnoreCase("maxproperty")) {
                customizedAggregationFunctions.add(new MaxProperty(argument.trim()));
            }
            if (function.trim().equalsIgnoreCase("sumproperty")) {
                customizedAggregationFunctions.add(new SumProperty(argument.trim()));
            }
        }
        return customizedAggregationFunctions;
    }
}
