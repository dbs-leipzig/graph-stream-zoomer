/*
 * Copyright © 2021 - 2023 Leipzig University (Database Research Group)
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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.util.HashMap;

/**
 * Parses the JSON incoming from the generator,
 * map it to the corresponding Stream Object instance.
 */
public class JSONToStreamObjectMapper implements MapFunction<String, StreamTriple> {

  /**
   * Field name of id
   */
  private static final String ID = "id";
  /**
   * Field name of label
   */
  private static final String LABEL = "label";
  /**
   * Field name of properties
   */
  private static final String PROPERTIES = "properties";
  /**
   * Field name of source
   */
  private static final String SOURCE = "source";
  /**
   * Field name of source
   */
  private static final String TARGET = "target";
  /**
   * Field name of timestamp
   */
  private static final String TIMESTAMP = "timestamp";

  public JSONToStreamObjectMapper() {
  }

  /**
   * converts the incoming value into its corresponding stream object
   *
   * @param jsonObjectAsString the incoming edge stream json object as String value
   * @return StreamObject:
   * Tuple6<id : String, timestamp : Long, label : String, properties : Properties, source : StreamVertex,
   * target : StreamVertex>
   */

  public StreamTriple map(String jsonObjectAsString) {
    JsonElement root = new JsonParser().parse(jsonObjectAsString);
    Gson g = new Gson();
    StreamVertex source;
    StreamVertex target;

    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    // new instance of StreamVertex as source object
    source = new StreamVertex(
      root.getAsJsonObject().get(SOURCE).getAsJsonObject().get(ID).getAsString(),
      root.getAsJsonObject().get(SOURCE).getAsJsonObject().get(LABEL).getAsString(),
      Properties.createFromMap(
        g.fromJson(
          root.getAsJsonObject().get(SOURCE).getAsJsonObject().get(PROPERTIES),
          new TypeToken<HashMap<String, Object>>() {}.getType()))
      ,timestamp);

    // new instance of StreamVertex as target object
    target = new StreamVertex(root.getAsJsonObject().get(TARGET).getAsJsonObject().get(ID).getAsString(),
      root.getAsJsonObject().get(TARGET).getAsJsonObject().get(LABEL).getAsString(), Properties.createFromMap(
      g.fromJson(root.getAsJsonObject().get(TARGET).getAsJsonObject().get(PROPERTIES),
        new TypeToken<HashMap<String, Object>>() {
        }.getType()))
      ,timestamp
    );

    // new instance of StreamObject as edge stream object
    return new StreamTriple(root.getAsJsonObject().get(ID).getAsString(),
      // root.getAsJsonObject().get(TIMESTAMP).getAsLong(),
      timestamp, root.getAsJsonObject().get(LABEL).getAsString(), Properties.createFromMap(
      g.fromJson(root.getAsJsonObject().get(PROPERTIES), new TypeToken<HashMap<String, Object>>() {
      }.getType())), source, target);
  }
}
