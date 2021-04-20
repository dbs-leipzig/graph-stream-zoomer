package edu.leipzig.impl.algorithm;

import edu.leipzig.model.graph.StreamEdge;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.graph.StreamTriple;
import edu.leipzig.model.graph.StreamVertex;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.gradoop.common.model.impl.properties.Properties;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraphStreamGroupingTest {

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
    new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberSlotsPerTaskManager(2)
        .setNumberTaskManagers(1)
        .build());

  @Test
  public void testSimpleGrouping() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(TimeUnit.SECONDS.toMillis(30));

    // Input data
    StreamVertex v1 = new StreamVertex("v1", "vertexLabel", Properties.create());
    StreamVertex v2 = new StreamVertex("v2", "vertexLabel", Properties.create());

    StreamTriple e1 = new StreamTriple("e1", 1618564800, "label", Properties.create(), v1, v2);
    StreamTriple e2 = new StreamTriple("e2", 1618564900, "label", Properties.create(), v1, v2);

    // Expected
    StreamVertex superV1 = new StreamVertex("unknown", "vertexLabel", Properties.create());
    StreamEdge superE1 = new StreamEdge("unknown", 1618564900L, "label", Properties.create(),
      superV1.getVertexId(), superV1.getVertexId());

    DataStream<StreamTriple> testStream = env.fromElements(e1, e2);

    StreamGraph streamGraph = StreamGraph.fromFlinkStream(testStream, new StreamGraphConfig(env, 1));

    streamGraph = streamGraph.groupBy(
      Collections.singletonList(":label"),
      null,
      Collections.singletonList(":label"),
      null);

    streamGraph.addVertexSink(new CollectVertexSink());
    streamGraph.addEdgeSink(new CollectEdgeSink());

    env.execute();

    assertEquals(1, CollectVertexSink.values.size());
    assertTrue(superV1.equalsWithoutId(CollectVertexSink.values.values().iterator().next()));

    assertEquals(1, CollectEdgeSink.values.size());
    assertTrue(superE1.equalsWithoutId(CollectEdgeSink.values.values().iterator().next()));
  }

  private static class CollectVertexSink implements SinkFunction<Tuple2<Boolean, StreamVertex>> {

    // must be static
    public static final Map<String, StreamVertex> values = Collections.synchronizedMap(new HashMap<>());

    @Override
    public void invoke(Tuple2<Boolean, StreamVertex> value, SinkFunction.Context context) throws Exception {
      if (value.f0) {
        values.put(value.f1.getVertexId(), value.f1);
      } else {
        values.remove(value.f1.getVertexId());
      }

    }
  }

  private static class CollectEdgeSink implements SinkFunction<Tuple2<Boolean, StreamEdge>> {

    // must be static
    public static final Map<String, StreamEdge> values = Collections.synchronizedMap(new HashMap<>());

    @Override
    public void invoke(Tuple2<Boolean, StreamEdge> value, SinkFunction.Context context) throws Exception {
      if (value.f0) {
        values.put(value.f1.getEdgeId(), value.f1);
      } else {
        values.remove(value.f1.getEdgeId());
      }

    }
  }
}