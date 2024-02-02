/*
 * Copyright © 2021 - 2024 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.dbsleipzig.stream.grouping.application.functions;

import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * A generator for some random stream triples of a common vertex dict.
 */
public class RandomStreamTripleGenerator extends RandomGenerator<StreamTriple> {

  /**
   * The dictionaries of labels and ids.
   */
  private final List<String> vertexLabelDict, edgeLabelDict, vertexIdDict;

  /**
   * Initialize the random generator.
   *
   * @param vertexLabels number of different vertex labels.
   * @param edgeLabels number of different edge labels.
   * @param vertexCount number of vertices that the stream generator should use as source/target.
   */
  public RandomStreamTripleGenerator(int vertexLabels, int edgeLabels, int vertexCount) {
    this.vertexLabelDict = new ArrayList<>();
    this.edgeLabelDict = new ArrayList<>();
    this.vertexIdDict = new ArrayList<>();

    for (int i = 0; i < vertexLabels; i++) {
      vertexLabelDict.add(RandomStringUtils.randomAlphabetic(3));
    }
    for (int i = 0; i < edgeLabels; i++) {
      edgeLabelDict.add(RandomStringUtils.randomAlphabetic(3));
    }
    for (int i = 0; i < vertexCount; i++) {
      vertexIdDict.add(UUID.randomUUID().toString());
    }
  }

  @Override
  public StreamTriple next() {
    // Generate unique edge id
    String edgeId = UUID.randomUUID().toString();

    // Get a random edge label
    String edgeLabel = getRandom(edgeLabelDict);

    // Get source and target vertex label and id
    String sourceLabel = getRandom(vertexLabelDict);
    String targetLabel = getRandom(vertexLabelDict);
    String sourceId = getRandom(vertexIdDict);
    String targetId = getRandom(vertexIdDict);

    // Get current time
    Timestamp time = new Timestamp(System.currentTimeMillis());

    // Generate some properties
    HashMap<String, Object> sourceVertexProperty = new HashMap<>();
    sourceVertexProperty.put("vertex_id_prov", sourceId);
    HashMap<String, Object> targetVertexProperty = new HashMap<>();
    targetVertexProperty.put("vertex_id_prov", targetId);
    HashMap<String, Object> propertiesForEdge = new HashMap<>();
    propertiesForEdge.put("edge_id_prov", edgeId);

    // Assign properties
    Properties sourceProps = Properties.createFromMap(sourceVertexProperty);
    Properties targetProps = Properties.createFromMap(targetVertexProperty);
    Properties edgeProperties = Properties.createFromMap(propertiesForEdge);

    // Create source and target vertex
    StreamVertex s = new StreamVertex(sourceId, sourceLabel, sourceProps, time);
    StreamVertex t = new StreamVertex(targetId, targetLabel, targetProps, time);

    //Create StreamTriple
    return new StreamTriple(edgeId, time, edgeLabel, edgeProperties, s, t);
  }

  /**
   * Get a random element from a list.
   *
   * @param list the list to query a random element.
   * @return the random element
   */
  private static String getRandom(List<String> list) {
    int r = new Random().nextInt(list.size());
    return list.get(r);
  }
}
