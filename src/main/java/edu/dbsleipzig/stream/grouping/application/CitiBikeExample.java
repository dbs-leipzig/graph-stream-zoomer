package edu.dbsleipzig.stream.grouping.application;

import edu.dbsleipzig.stream.grouping.impl.algorithm.TableGroupingBase;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.AvgProperty;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.MinProperty;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class CitiBikeExample {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("taskmanager.memory.network.max", "1gb");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);

        DataStream<StreamTriple> citiBikeStream = createInputFromCsv(env);

        StreamGraph streamGraph = StreamGraph.fromFlinkStream(citiBikeStream, new StreamGraphConfig(env));

        TableGroupingBase.GroupingBuilder groupingBuilder = new TableGroupingBase.GroupingBuilder();

        groupingBuilder.addVertexGroupingKey(":label");
        groupingBuilder.addEdgeGroupingKey(":label");
        groupingBuilder.addEdgeAggregateFunction(new AvgProperty("tripduration"));
        //groupingBuilder.addEdgeAggregateFunction(new MinProperty("birth year"));

        streamGraph = groupingBuilder.build().execute(streamGraph);

        streamGraph.printVertices();
        streamGraph.printEdges();

        env.execute();
    }

    public static DataStream<StreamTriple> createInputFromCsv(StreamExecutionEnvironment env) {

        TupleTypeInfo<Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> tuple15TupleTypeInfo =
                TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(String.class, String.class, String.class, String.class, String.class,
                        String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);


        CsvInputFormat<Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> inputFormat
                = new TupleCsvInputFormat<>(new Path("./src/main/resources/citibike-data/201306-citibike-tripdata.csv"), tuple15TupleTypeInfo);
        inputFormat.setSkipFirstLineAsHeader(true);
        DataStreamSource<Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                source = env.createInput(inputFormat, tuple15TupleTypeInfo);

        DataStream<StreamTriple> tripleDataStream = source.map((MapFunction<Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>, StreamTriple>) tuple15 -> {
            StreamTriple streamTriple = new StreamTriple();
            StreamVertex sourceVertex = new StreamVertex();
            StreamVertex targetVertex = new StreamVertex();

            HashMap<String, Object> edgePropMap = new HashMap<>();
            HashMap<String, Object> sourcePropMap = new HashMap<>();
            HashMap<String, Object> targetPropMap = new HashMap<>();

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date parseStopTime = simpleDateFormat.parse(tuple15.f2.replaceAll("\"", ""));
            Timestamp stopTime = new Timestamp(parseStopTime.getTime());
            edgePropMap.put("starttime", tuple15.f1);
            edgePropMap.put("stoptime", tuple15.f2);
            streamTriple.setTimestamp(stopTime);
            sourceVertex.setEventTime(stopTime);
            targetVertex.setEventTime(stopTime);
            sourceVertex.setVertexId(tuple15.f3);
            sourceVertex.setVertexLabel(tuple15.f4);
            sourcePropMap.put("lat", Double.parseDouble(tuple15.f5));
            sourcePropMap.put("long", Double.parseDouble(tuple15.f6));
            targetVertex.setVertexId(tuple15.f7);
            targetVertex.setVertexLabel(tuple15.f8);
            if (!tuple15.f9.equalsIgnoreCase("NULL")){
                targetPropMap.put("lat", Double.parseDouble(tuple15.f9));
            }
            if (!tuple15.f10.equalsIgnoreCase("NULL")) {
                targetPropMap.put("long", Double.parseDouble(tuple15.f10));
            }
            streamTriple.setId(tuple15.f11);
            streamTriple.setLabel(tuple15.f12);
            if (tuple15.f13 != null && !tuple15.f13.equals("NULL")) {
                edgePropMap.put("birth year", Integer.parseInt(tuple15.f13));
            }
            edgePropMap.put("gender", tuple15.f14);;
            sourceVertex.setVertexProperties(Properties.createFromMap(sourcePropMap));
            targetVertex.setVertexProperties(Properties.createFromMap(targetPropMap));
            streamTriple.setSource(sourceVertex);
            streamTriple.setTarget(targetVertex);

            return streamTriple;
        });


        return tripleDataStream;
    }
}
