package edu.leipzig.model.graph;

import edu.leipzig.impl.algorithm.GraphStreamGrouping;
import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.impl.functions.utils.PlannerExpressionBuilder;
import edu.leipzig.impl.functions.utils.PlannerExpressionSeqBuilder;
import edu.leipzig.model.table.TableSet;
import edu.leipzig.model.table.TableSetFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;

import java.util.List;
import java.util.Objects;

/**
 * layout of the stream graph
 * A stream graph layout is wrapping a {@link TableSet} which defines, how the layout is
 * represented in Apache Flink table API.
 * here we three tables (edges, vertices , graph)
 */

public class StreamGraphLayout {
  /**
   * Stream graph Configuration
   */
  private final StreamGraphConfig config;

  /**
   * Table set the layout is based on
   */
  private final TableSet tableSet;
  /**
   * Factory used to product instances of table set the layout is based on
   */
  private final TableSetFactory tableSetFactory;

  public StreamGraphLayout(DataStream<StreamVertex> vertices, DataStream<StreamEdge> edges,
    StreamGraphConfig config) {
    TableSet tableSet = new TableSet();
    tableSet.put(TableSet.TABLE_VERTICES, config.getTableEnvironment().fromDataStream(vertices));
    tableSet.put(TableSet.TABLE_EDGES, config.getTableEnvironment().fromDataStream(edges));
    this.tableSet = tableSet;
    this.config = Objects.requireNonNull(config);
    this.tableSetFactory = new TableSetFactory();
  }


  /**
   * Constructor
   *
   * @param tableSet table set
   * @param config   graph stream configuration
   */
  public StreamGraphLayout(TableSet tableSet, StreamGraphConfig config) {
    this.tableSet = tableSet;
    this.config = config;
    this.tableSetFactory = new TableSetFactory();
  }

  /**
   * Returns the stream graph Configuration
   *
   * @return stream graph Configuration
   */
  public StreamGraphConfig getConfig() {
    return config;
  }

  /**
   * Returns the table set .
   *
   * @return table set.
   */
  public TableSet getTableSet() {
    return tableSet;
  }

  /**
   * Returns the table set factory.
   *
   * @return table set factory.
   */
  public TableSetFactory getTableSetFactory() {
    return tableSetFactory;
  }

  /**
   * Creates a condensed version of the stream graph by grouping vertices based on the specified
   * property keys.
   * <p>
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @return summary graph
   * @see GraphStreamGrouping
   */
  StreamGraphLayout groupBy(List<String> vertexGroupingKeys) {
    return groupBy(vertexGroupingKeys, null);
  }

  /**
   * Creates a condensed version of the stream graph by grouping vertices and edges based on given
   * property keys.
   * <p>
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys.
   * <p>
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param edgeGroupingKeys   property keys to group edges
   * @return summary graph
   * @see GraphStreamGrouping
   */
  StreamGraphLayout groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
    return groupBy(vertexGroupingKeys, null, edgeGroupingKeys, null);
  }

  /**
   * Creates a condensed version of the stream graph by grouping vertices and edges based on given
   * property keys.
   * <p>
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys. Furthermore, one can
   * specify sets of vertex and edge aggregate functions which are applied on vertices/edges
   * represented by the same super vertex/edge.
   * <p>
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   *
   * @param vertexGroupingKeys       property keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys         property keys to group edges
   * @param edgeAggregateFunctions   aggregate functions to apply on super edges
   * @return summary graph
   * @see GraphStreamGrouping
   */
  StreamGraphLayout groupBy(
    List<String> vertexGroupingKeys,
    List<CustomizedAggregationFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys,
    List<CustomizedAggregationFunction> edgeAggregateFunctions
  ) {
    Objects.requireNonNull(vertexGroupingKeys, "missing vertex grouping key(s)");

    GraphStreamGrouping.GroupingBuilder builder = new GraphStreamGrouping.GroupingBuilder();

    builder.addVertexGroupingKeys(vertexGroupingKeys);

    if (edgeGroupingKeys != null) {
      builder.addEdgeGroupingKeys(edgeGroupingKeys);
    }

    if (vertexAggregateFunctions != null) {
      for (CustomizedAggregationFunction f : vertexAggregateFunctions) {
        builder.addVertexAggregateFunction(f);
      }
    }

    if (edgeAggregateFunctions != null) {
      for (CustomizedAggregationFunction f : edgeAggregateFunctions) {
        builder.addEdgeAggregateFunction(f);
      }
    }

    return builder.build().execute(this);
  }

  /**
   * Computes vertex induced edges by performing
   * <p>
   * (Edges ⋈ Vertices on head_id=vertex_id)
   * ⋈ Vertices on tail_id=vertex_id)
   *
   * @param edges    original edges table
   * @param vertices inducing vertices table
   * @return table
   */
  Table computeSummarizedGraphTable(Table edges, Table vertices) {
    String newId1 = config.createUniqueAttributeName();
    String newId2 = config.createUniqueAttributeName();
    PlannerExpressionBuilder builder = new PlannerExpressionBuilder(config.getTableEnvironment());

    return tableSet.projectToGraph(
      edges
        .join(
          vertices.select(new PlannerExpressionSeqBuilder(config.getTableEnvironment())
            .field(TableSet.FIELD_VERTEX_ID)
            .as(newId1)
            .field(TableSet.FIELD_VERTEX_LABEL)
            .as(TableSet.FIELD_VERTEX_SOURCE_LABEL)
            .field(TableSet.FIELD_VERTEX_PROPERTIES)
            .as(TableSet.FIELD_VERTEX_SOURCE_PROPERTIES).build()),
          builder.field(TableSet.FIELD_TAIL_ID)
            .equalTo(newId1).getExpression())
        .join(vertices.select(new PlannerExpressionSeqBuilder(config.getTableEnvironment())
            .field(TableSet.FIELD_VERTEX_ID)
            .as(newId2)
            .field(TableSet.FIELD_VERTEX_LABEL)
            .as(TableSet.FIELD_VERTEX_TARGET_LABEL)
            .field(TableSet.FIELD_VERTEX_PROPERTIES)
            .as(TableSet.FIELD_VERTEX_TARGET_PROPERTIES).build()),
          builder.field(TableSet.FIELD_HEAD_ID)
            .equalTo(newId2).getExpression()));
  }
}
