package edu.leipzig.impl.algorithm;

import edu.leipzig.impl.functions.utils.CreateSuperElementId;
import edu.leipzig.impl.functions.utils.ToProperties;
import edu.leipzig.model.graph.StreamEdge;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.graph.StreamTriple;
import edu.leipzig.model.graph.StreamVertex;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static edu.leipzig.impl.algorithm.TableGroupingBase.FIELD_SUPER_VERTEX_ID;
import static edu.leipzig.impl.algorithm.TableGroupingBase.FIELD_SUPER_VERTEX_LABEL;
import static edu.leipzig.model.table.TableSet.*;
import static org.apache.flink.table.api.Expressions.*;
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

    Timestamp t1 = new Timestamp(1619511681000L);
    Timestamp t2 = new Timestamp(1619511682000L);
    Timestamp t3 = new Timestamp(1619511683000L);
    Timestamp t4 = new Timestamp(1619511684000L);

    // Input data
    StreamVertex v11 = new StreamVertex("v1", "A", Properties.create()
      ,t1
    );
    StreamVertex v12 = new StreamVertex("v2", "B", Properties.create()
      , t1
    );

    StreamVertex v21 = new StreamVertex("v1", "A", Properties.create()
      , t2
    );
    StreamVertex v22 = new StreamVertex("v2", "B", Properties.create()
      , t2
      );

    StreamVertex v31 = new StreamVertex("v1", "A", Properties.create()
      , t3
    );
    StreamVertex v32 = new StreamVertex("v2", "B", Properties.create()
      , t3
    );

    StreamVertex v41 = new StreamVertex("v1", "A", Properties.create()
      , t4
    );
    StreamVertex v42 = new StreamVertex("v2", "B", Properties.create()
      , t4
    );

    StreamTriple e1 = new StreamTriple("e1", t1, "label", Properties.create(), v11, v12);
    StreamTriple e2 = new StreamTriple("e2", t2, "label", Properties.create(), v21, v22);
    StreamTriple e3 = new StreamTriple("e3", t3, "label", Properties.create(), v31, v32);
    StreamTriple e4 = new StreamTriple("e4", t4, "label", Properties.create(), v41, v42);

    // Expected
    StreamVertex superV1 = new StreamVertex("unknown", "A", Properties.create()
      , t4
      );
    StreamVertex superV2 = new StreamVertex("unknown", "B", Properties.create()
      , t4
    );
    StreamEdge superE1 = new StreamEdge("unknown", t4, "label", Properties.create(),
      superV1.getVertexId(), superV2.getVertexId());

    DataStream<StreamTriple> testStream = env.fromElements(e1, e2, e3, e4)
      .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .<StreamTriple>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime()));

    StreamGraph streamGraph = StreamGraph.fromFlinkStream(testStream, new StreamGraphConfig(env));

    streamGraph = streamGraph.groupBy(
      Collections.singletonList(":label"),
      null,
      Collections.singletonList(":label"),
      null);

    //streamGraph.addVertexSink(new CollectVertexSink());
   // streamGraph.addEdgeSink(new CollectEdgeSink());

    //env.execute();

    //assertEquals(1, CollectVertexSink.values.size());
    //assertTrue(superV1.equalsWithoutId(CollectVertexSink.values.values().iterator().next()));
/*
    assertEquals(1, CollectEdgeSink.values.size());
    assertTrue(superE1.equalsWithoutId(CollectEdgeSink.values.values().iterator().next()));*/
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

  @Test
  public void testDoubleGrouping() throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    final StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, bsSettings);

    Timestamp t1 = new Timestamp(1619511681000L);
    Timestamp t2 = new Timestamp(1619511682000L);
    Timestamp t3 = new Timestamp(1619511683000L);
    Timestamp t4 = new Timestamp(1619511684000L);

    // Input data, StreamVertex(vertex_id, vertex_label, vertex_properties, event_time)
    StreamVertex v11 = new StreamVertex("v1", "A", Properties.create(), t1);
    StreamVertex v12 = new StreamVertex("v2", "B", Properties.create(), t1);
    StreamVertex v21 = new StreamVertex("v1", "A", Properties.create(), t2);
    StreamVertex v22 = new StreamVertex("v2", "B", Properties.create(), t2);
    StreamVertex v31 = new StreamVertex("v1", "A", Properties.create(), t3);
    StreamVertex v32 = new StreamVertex("v2", "B", Properties.create(), t3);
    StreamVertex v41 = new StreamVertex("v1", "A", Properties.create(), t4);
    StreamVertex v42 = new StreamVertex("v2", "B", Properties.create(), t4);

    String ID = FIELD_VERTEX_ID;
    String LABEL = FIELD_VERTEX_LABEL;
    String PROPERTIES = FIELD_VERTEX_PROPERTIES;
    String EVENT_TIME = FIELD_EVENT_TIME;

    CreateSuperElementId f1 = new CreateSuperElementId();

    streamTableEnvironment.registerFunction(f1.toString(), f1);

    // Put input data to table
    Table vertices = streamTableEnvironment.fromDataStream(
      env
        .fromElements(v11, v12, v21, v22, v31, v32, v41, v42)
        .assignTimestampsAndWatermarks(WatermarkStrategy.<StreamVertex>forBoundedOutOfOrderness(Duration.ofSeconds(5))
          .withTimestampAssigner((event, timestamp) -> event.getEventTime().getTime())),
      // Expressions with declaration of 'event_time' as rowtime
      $(ID), $(LABEL), $(PROPERTIES), $(EVENT_TIME).rowtime());

// 1. Prepare distinct vertices
    Table preparedVertices = vertices
      .window(Tumble.over(lit(10).seconds()).on($(EVENT_TIME)).as("w1"))
      .groupBy($(ID), $(LABEL), $("w1"))
      .select($(ID), $(LABEL), lit(5L).as("prop"), $("w1").rowtime().as("w1_rowtime"));

 //preparedVertices.execute().print(); //--> would work well

// 2. Group vertices by label and/or property values
    Table groupedVertices = preparedVertices
      .window(Tumble.over(lit(10).seconds()).on($("w1_rowtime")).as("w2"))
      .groupBy($(LABEL), $("w2"))
      .select(
        $(LABEL).as("super_label"),
        lit(1).count().as("super_count"),
        $("w2").rowtime().as("w2_rowtime")
      );

 //groupedVertices.execute().print(); //--> would work well

   // streamTableEnvironment.toAppendStream(groupedVertices, Row.class).print();
    groupedVertices
      .select($("super_label"), $("w2_rowtime"))
      .execute().print(); // --> throws exception*/
  }
}