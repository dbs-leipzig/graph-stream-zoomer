package edu.leipzig.application.functions;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import edu.leipzig.model.graph.StreamObject;
import edu.leipzig.model.graph.StreamVertex;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.util.HashMap;

/**
 * Parses the JSON incoming from the generator,
 * map it to the corresponding Stream Object instance.
 */
public class JSONToStreamObjectMapper implements MapFunction<String, StreamObject> {

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

  public StreamObject map(String jsonObjectAsString) {
    JsonElement root = new JsonParser().parse(jsonObjectAsString);
    Gson g = new Gson();
    StreamVertex source;
    StreamVertex target;

    // new instance of StreamVertex as source object
    source = new StreamVertex(root.getAsJsonObject().get(SOURCE).getAsJsonObject().get(ID).getAsString(),
      root.getAsJsonObject().get(SOURCE).getAsJsonObject().get(LABEL).getAsString(), Properties.createFromMap(
      g.fromJson(root.getAsJsonObject().get(SOURCE).getAsJsonObject().get(PROPERTIES),
        new TypeToken<HashMap<String, Object>>() {
        }.getType())));

    // new instance of StreamVertex as target object
    target = new StreamVertex(root.getAsJsonObject().get(TARGET).getAsJsonObject().get(ID).getAsString(),
      root.getAsJsonObject().get(TARGET).getAsJsonObject().get(LABEL).getAsString(), Properties.createFromMap(
      g.fromJson(root.getAsJsonObject().get(TARGET).getAsJsonObject().get(PROPERTIES),
        new TypeToken<HashMap<String, Object>>() {
        }.getType())));
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    // new instance of StreamObject as edge stream object
    return new StreamObject(root.getAsJsonObject().get(ID).getAsString(),
      // root.getAsJsonObject().get(TIMESTAMP).getAsLong(),
      timestamp.getTime(), root.getAsJsonObject().get(LABEL).getAsString(), Properties.createFromMap(
      g.fromJson(root.getAsJsonObject().get(PROPERTIES), new TypeToken<HashMap<String, Object>>() {
      }.getType())), source, target);
  }
}
