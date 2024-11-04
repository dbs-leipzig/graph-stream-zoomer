package edu.dbsleipzig.stream.grouping.application.functions.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;
import java.util.HashMap;

public class StreamTripleToJsonStringMapFunction implements Serializable, MapFunction<StreamTriple, String> {
  @Override
  public String map(StreamTriple value) throws Exception {
    // not cool to init every time, but since it is not serializable, only way atm
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
}
