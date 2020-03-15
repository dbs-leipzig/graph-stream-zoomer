package edu.leipzig.model.streamGraph;

import edu.leipzig.impl.algorithm.GraphSummarizer;
import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.impl.functions.utils.ExpressionBuilder;
import edu.leipzig.impl.functions.utils.ExpressionSeqBuilder;
import edu.leipzig.model.table.TableSet;
import edu.leipzig.model.table.TableSetFactory;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.expressions.Expression;

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
    private StreamGraphConfig config;

    /**
     * Table set the layout is based on
     */
    private TableSet tableSet;
    /**
     * Factory used to product instances of table set the layout is based on
     */
    private TableSetFactory tableSetFactory;

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
     * Returns the  Stream Graph Layout.
     *
     * @return Stream Graph Layout.
     */
    StreamGraphLayout groupBy(List<String> vertexGroupingKeys) {
        return groupBy(vertexGroupingKeys, null);
    }

    /**
     * Returns the  Stream Graph Layout.
     *
     * @return Stream Graph Layout.
     */
    StreamGraphLayout groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
        return groupBy(vertexGroupingKeys, null, edgeGroupingKeys, null);
    }

    /**
     * Returns the  Stream Graph Layout.
     *
     * @return Stream Graph Layout.
     */
    StreamGraphLayout groupBy(List<String> vertexGroupingKeys,
                              List<CustomizedAggregationFunction> vertexAggregateFunctions, List<String> edgeGroupingKeys,
                              List<CustomizedAggregationFunction> edgeAggregateFunctions) {
        Objects.requireNonNull(vertexGroupingKeys, "missing vertex grouping key(s)");

        GraphSummarizer.GroupingBuilder builder = new GraphSummarizer.GroupingBuilder();

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

        return new StreamGraphLayout(builder.build().execute(this), config);
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
    Table computeSummarizedGraphTable(Table edges,
                                      Table vertices) {
        String newId1 = config.createUniqueAttributeName();
        String newId2 = config.createUniqueAttributeName();
        ExpressionBuilder builder = new ExpressionBuilder();

        return tableSet.projectToGraph(
                edges
                        .join(vertices
                                .select((Expression[])  new ExpressionSeqBuilder()
                                        .field(TableSet.FIELD_VERTEX_ID).as(newId1)
                                        .field(TableSet.FIELD_VERTEX_LABEL).as(TableSet.FIELD_VERTEX_SOURCE_LABEL)
                                        .field(TableSet.FIELD_VERTEX_PROPERTIES).as(TableSet.FIELD_VERTEX_SOURCE_PROPERTIES)
                                        .buildList().toArray()
                                ), builder
                                .field(TableSet.FIELD_TAIL_ID)
                                .equalTo(newId1)
                                .toExpression()
                        ).join(vertices
                        .select((Expression[])  new ExpressionSeqBuilder()
                                .field(TableSet.FIELD_VERTEX_ID).as(newId2)
                                .field(TableSet.FIELD_VERTEX_LABEL).as(TableSet.FIELD_VERTEX_TARGET_LABEL)
                                .field(TableSet.FIELD_VERTEX_PROPERTIES).as(TableSet.FIELD_VERTEX_TARGET_PROPERTIES)
                                .buildList().toArray()
                        ), builder
                        .field(TableSet.FIELD_HEAD_ID)
                        .equalTo(newId2)
                        .toExpression()
                )
        );

    }
}
