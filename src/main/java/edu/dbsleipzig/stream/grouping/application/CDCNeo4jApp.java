package edu.dbsleipzig.stream.grouping.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.dbsleipzig.stream.grouping.application.functions.CDCEventToTripleMapper;
import edu.dbsleipzig.stream.grouping.application.functions.events.CDCEvent;
import edu.dbsleipzig.stream.grouping.application.functions.events.EventDeserializationSchema;
import edu.dbsleipzig.stream.grouping.impl.algorithm.GraphStreamGrouping;
import edu.dbsleipzig.stream.grouping.impl.algorithm.TableGroupingBase;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.AvgProperty;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.Count;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.MaxProperty;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.MinProperty;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.WindowConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinEdgeProperty;

import java.util.HashMap;
import java.util.HashSet;


public class CDCNeo4jApp {

  public static void main(String[] args) throws Exception {

    String TOPIC = "creates";

    KafkaSource<CDCEvent> source =
      KafkaSource.<CDCEvent>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics(TOPIC)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new EventDeserializationSchema()))
        .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setParallelism(1);

    DataStream<CDCEvent> kafkaStringStream = env.fromSource(
      source, WatermarkStrategy.forMonotonousTimestamps(), "KafkaSource");

    DataStream<StreamTriple> graphStreamTriples = kafkaStringStream.flatMap(new CDCEventToTripleMapper());

    StreamGraph streamGraph = StreamGraph.fromFlinkStream(graphStreamTriples, new StreamGraphConfig(env));

    // Configure and build the grouping operator
    GraphStreamGrouping groupingOperator = new TableGroupingBase.GroupingBuilder()
      .setWindowSize(60, WindowConfig.TimeUnit.SECONDS)
      .addVertexGroupingKey(":label")
      .addVertexGroupingKey("id")
      .addEdgeGroupingKey(":label")
      .addEdgeGroupingKey("operation")
      //.addVertexAggregateFunction(new Count())
      .addEdgeAggregateFunction(new Count())
      .addEdgeAggregateFunction(new MinProperty("duration", "minDuration"))
      .addEdgeAggregateFunction(new MaxProperty( "duration", "maxDuration"))
      .addEdgeAggregateFunction(new AvgProperty( "duration", "avgDuration"))
      .build();

    // Execute the grouping and overwrite the input stream with the grouping result
    streamGraph = groupingOperator.execute(streamGraph);

    // Print on console
    streamGraph.print();

    streamGraph.toTripleStream().map(new MapFunction<StreamTriple, String>() {
      @Override
      public String map(StreamTriple value) throws Exception {

        ObjectMapper objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());

        JsonNode object = objectMapper.valueToTree(value);

        JsonNode srcVertex = object.get("source");
        JsonNode trgVertex = object.get("target");



        HashMap<String, String> sourcePropertyMap = new HashMap<>();
        value.getSource().getVertexProperties().iterator().forEachRemaining(property -> {
          sourcePropertyMap.put(property.getKey(), property.getValue().toString());
        });

        HashMap<String, String> targetPropertyMap = new HashMap<>();
        value.getTarget().getVertexProperties().iterator().forEachRemaining(property -> {
          targetPropertyMap.put(property.getKey(), property.getValue().toString());
        });

        ((ObjectNode) srcVertex).put("vertexProperties", objectMapper.valueToTree(sourcePropertyMap));
        ((ObjectNode) trgVertex).put("vertexProperties", objectMapper.valueToTree(targetPropertyMap));

        HashMap<String, String> edgePropertyMap = new HashMap<>();
        value.getProperties().iterator().forEachRemaining(property -> {
          edgePropertyMap.put(property.getKey(), property.getValue().toString());
        });

        ((ObjectNode) object).put("properties", objectMapper.valueToTree(edgePropertyMap));

        return object.toString();

      }
    });
    //  .print();

    // Write to Socket
//    SocketClientSink<String> socketSink = new SocketClientSink<>("localhost", 19093, new SimpleStringSchema());
//
//    streamGraph
//      .toTripleStream()
//      .map(StreamTriple::toString)
//      .returns(TypeInformation.of(String.class))
//      .addSink(socketSink);

    // Trigger the workflow execution
    env.execute();
  }

}
