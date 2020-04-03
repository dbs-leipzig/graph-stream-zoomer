package edu.leipzig.impl.algorithm;

import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.impl.functions.utils.EmptyProperties;
import edu.leipzig.impl.functions.utils.ExtractPropertyValue;
import edu.leipzig.impl.functions.utils.PlannerExpressionBuilder;
import edu.leipzig.impl.functions.utils.PlannerExpressionSeqBuilder;
import edu.leipzig.impl.functions.utils.ToProperties;
import edu.leipzig.model.streamGraph.StreamGraphLayout;
import edu.leipzig.model.table.TableSet;
import edu.leipzig.model.table.TableSetFactory;
import org.apache.flink.table.api.Table;
import org.gradoop.common.util.GradoopConstants;

import java.util.List;
import java.util.Map;

import static edu.leipzig.model.table.TableSet.TABLE_EDGES;
import static edu.leipzig.model.table.TableSet.TABLE_VERTICES;

/**
 * Implementation of grouping in a graph stream layout.
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has some changes.
 * these changes are related to using data stream instead of data set as data structure in Grable.
 *
 * @link Grouping
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.gve.operators;
 */

public class GraphSummarizer extends TableGroupingBase {

    /**
     * Creates grouping operator instance.
     *
     * @param useVertexLabels            group on vertex label true/false
     * @param useEdgeLabels              group on edge label true/false
     * @param vertexGroupingPropertyKeys list of property keys to group vertices by
     * @param vertexAggregateFunctions   aggregate functions to execute on grouped vertices
     * @param edgeGroupingPropertyKeys   list of property keys to group edges by
     * @param edgeAggregateFunctions     aggregate functions to execute on grouped edges
     */
    private GraphSummarizer(boolean useVertexLabels, boolean useEdgeLabels,
                            List<String> vertexGroupingPropertyKeys, List<CustomizedAggregationFunction> vertexAggregateFunctions,
                            List<String> edgeGroupingPropertyKeys, List<CustomizedAggregationFunction> edgeAggregateFunctions) {
        super(useVertexLabels, useEdgeLabels, vertexGroupingPropertyKeys, vertexAggregateFunctions,
                edgeGroupingPropertyKeys, edgeAggregateFunctions);
    }

    /**
     * The actual computation.
     *
     * @param streamGraphLayout layout of the stream graph
     * @return summarized, aggregated graph table set (super vertices, super edges)
     */
    public TableSet execute(StreamGraphLayout streamGraphLayout) {
        tableSetFactory = streamGraphLayout.getTableSetFactory();
        config = streamGraphLayout.getConfig();
        tableEnv = config.getTableEnvironment();
        builder = new PlannerExpressionBuilder(tableEnv);
        this.tableSet = streamGraphLayout.getTableSet();

        return performGrouping();
    }


    /**
     * Perform grouping based on stream graph layout and put result tables into new table set
     *
     * @return table set of result stream graph
     */
    protected TableSet performGrouping() {
        tableSetFactory = new TableSetFactory();

        tableEnv.createTemporaryView(TABLE_VERTICES, tableSet.getVertices());
        tableEnv.createTemporaryView(TABLE_EDGES, tableSet.getEdges());

        // 1. Prepare distinct vertices
        Table preparedVertices = extractVertexPropertyValuesAsColumns().distinct();

        // 2. Group vertices by label and/or property values
        Table groupedVertices = preparedVertices
                .groupBy(buildVertexGroupExpressions())
                .select(buildVertexProjectExpressions());

        // 3. Derive new super vertices
        Table newVertices = groupedVertices
                .select(buildSuperVertexProjectExpressions());

        // 4. Expand a (vertex -> super vertex) mapping
        Table expandedVertices = joinVerticesWithGroupedVertices(preparedVertices, groupedVertices);

        // 5. Assign super vertices to edges
        Table edgesWithSuperVertices = enrichEdges(tableSet.getEdges(), expandedVertices);

        // 6. Group edges by label and/or property values
        Table groupedEdges = edgesWithSuperVertices
                .groupBy(buildEdgeGroupExpressions())
                .select(buildEdgeProjectExpressions());

        // 7. Derive new super edges from grouped edges
        Table newEdges = groupedEdges.select(buildSuperEdgeProjectExpressions());

        return tableSetFactory.fromTables(newVertices, newEdges);
    }

    /**
     * Projects needed property values from properties instance into own fields for each property.
     *
     * @return prepared vertices table
     */
    private Table extractVertexPropertyValuesAsColumns() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(tableEnv);

        // vertex_id
        builder.field(TableSet.FIELD_VERTEX_ID);

        // optionally: vertex_label
        if (useVertexLabels) {
            builder.field(TableSet.FIELD_VERTEX_LABEL);
        }

        // grouping_property_1 AS tmp_a1, ... , grouping_property_n AS tmp_an
        for (String propertyKey : vertexGroupingPropertyKeys) {
            String propertyFieldName = config.createUniqueAttributeName();
            builder
                    .scalarFunctionCall(new ExtractPropertyValue(propertyKey),
                            TableSet.FIELD_VERTEX_PROPERTIES)
                    .as(propertyFieldName);
            vertexGroupingPropertyFieldNames.put(propertyKey, propertyFieldName);
        }

        // property_to_aggregate_1 AS tmp_b1, ... , property_to_aggregate_m AS tmp_bm
        for (CustomizedAggregationFunction aggregateFunction : vertexAggregateFunctions) {
            if (null != aggregateFunction.getPropertyKey()) {
                String propertyFieldName = config.createUniqueAttributeName();
                builder.scalarFunctionCall(new ExtractPropertyValue(aggregateFunction.getPropertyKey()),
                        TableSet.FIELD_VERTEX_PROPERTIES).as(propertyFieldName);
                vertexAggregationPropertyFieldNames
                        .put(aggregateFunction.getAggregatePropertyKey(), propertyFieldName);
            }
        }

        return tableSet.getVertices().select(builder.buildString());
    }

    /**
     * Collects all expressions the grouped vertex table gets projected to in order to select super
     * vertices
     * <p>
     * { vertex_id, vertex_label, vertex_properties }
     *
     * @return scala sequence of expressions
     */
    private String buildSuperVertexProjectExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(tableEnv);

        // vertex_id
        builder.field(FIELD_SUPER_VERTEX_ID).as(TableSet.FIELD_VERTEX_ID);

        // vertex_label
        if (useVertexLabels) {
            builder.field(FIELD_SUPER_VERTEX_LABEL).as(TableSet.FIELD_VERTEX_LABEL);
        } else {
            builder
                    .literal(GradoopConstants.DEFAULT_VERTEX_LABEL)
                    .as(TableSet.FIELD_VERTEX_LABEL);
        }

        // grouped_properties + aggregated_properties -> vertex_properties
        PlannerExpressionSeqBuilder propertyKeysAndFieldsBuilder = new PlannerExpressionSeqBuilder(tableEnv);
        addPropertyKeyValueExpressions(
                propertyKeysAndFieldsBuilder,
                vertexGroupingPropertyKeys, vertexAfterGroupingPropertyFieldNames
        );
        addPropertyKeyValueExpressions(
                propertyKeysAndFieldsBuilder,
                getVertexAggregatedPropertyKeys(), vertexAfterAggregationPropertyFieldNames
        );

        if (propertyKeysAndFieldsBuilder.isEmpty()) {
            builder.scalarFunctionCall(new EmptyProperties());
        } else {
            builder.scalarFunctionCall(new ToProperties(),
              (new PlannerExpressionBuilder(tableEnv)).row(propertyKeysAndFieldsBuilder.buildString()).getExpression());
        }

        builder.as(TableSet.FIELD_VERTEX_PROPERTIES);

        return builder.buildString();
    }

    /**
     * Collects all expressions the grouped edge table gets projected to in order to select super
     * edges
     * <p>
     * { edge_id, tail_id, head_id, edge_label, edge_properties }
     *
     * @return scala sequence of expressions
     */
    private String buildSuperEdgeProjectExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(tableEnv);

        // edge_id, tail_id, head_id
        builder
                .field(FIELD_SUPER_EDGE_ID).as(TableSet.FIELD_EDGE_ID)
                .field(TableSet.FIELD_TAIL_ID)
                .field(TableSet.FIELD_HEAD_ID);

        // edge_label
        if (useEdgeLabels) {
            builder.field(TableSet.FIELD_EDGE_LABEL);
        } else {
            builder
                    .literal(GradoopConstants.DEFAULT_EDGE_LABEL)
                    .as(TableSet.FIELD_EDGE_LABEL);
        }

        // grouped_properties + aggregated_properties -> edge_properties
        PlannerExpressionSeqBuilder propertyKeysAndFieldsBuilder = new PlannerExpressionSeqBuilder(tableEnv);
        addPropertyKeyValueExpressions(
                propertyKeysAndFieldsBuilder,
                edgeGroupingPropertyKeys, edgeAfterGroupingPropertyFieldNames
        );
        addPropertyKeyValueExpressions(
                propertyKeysAndFieldsBuilder,
                getEdgeAggregatedPropertyKeys(), edgeAfterAggregationPropertyFieldNames
        );

        if (propertyKeysAndFieldsBuilder.isEmpty()) {
            builder.scalarFunctionCall(new EmptyProperties());
        } else {
            builder.scalarFunctionCall(new ToProperties(),
              (new PlannerExpressionBuilder(tableEnv)).row(propertyKeysAndFieldsBuilder.buildString()).getExpression());
        }
        builder.as(TableSet.FIELD_EDGE_PROPERTIES);

        return builder.buildString();
    }

    @Override
    protected Table enrichEdges(Table edges, Table expandedVertices,
                                String... additionalProjectExpressions) {

        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(tableEnv);

        // grouping_property_1 AS tmp_e1, ... , grouping_property_k AS tmp_ek
        for (String propertyKey : edgeGroupingPropertyKeys) {
            String propertyColumnName = config.createUniqueAttributeName();
            builder
                    .scalarFunctionCall(new ExtractPropertyValue(propertyKey),
                            TableSet.FIELD_EDGE_PROPERTIES)
                    .as(propertyColumnName);
            edgeGroupingPropertyFieldNames.put(propertyKey, propertyColumnName);
        }

        // property_to_aggregate_1 AS tmp_f1, ... , property_to_aggregate_l AS tmp_fl
        for (CustomizedAggregationFunction aggregateFunction : edgeAggregateFunctions) {
            if (null != aggregateFunction.getPropertyKey()) {
                String propertyColumnName = config.createUniqueAttributeName();
                builder.scalarFunctionCall(new ExtractPropertyValue(aggregateFunction.getPropertyKey()),
                        TableSet.FIELD_EDGE_PROPERTIES)
                        .as(propertyColumnName);
                edgeAggregationPropertyFieldNames.put(aggregateFunction.getAggregatePropertyKey(),
                        propertyColumnName);
            }
        }

        return super.enrichEdges(edges, expandedVertices, builder.buildString());
    }

    /**
     * Takes an expression sequence builder and adds following expressions for each of given
     * property keys to the builder:
     * <p>
     * LITERAL('property_key_1'), field_name_of_property_1, ... , LITERAL('property_key_n'),
     * field_name_of_property_n
     *
     * @param builder      expression sequence builder to add expressions to
     * @param propertyKeys property keys
     * @param fieldNameMap map of field names properties
     */
    private void addPropertyKeyValueExpressions(PlannerExpressionSeqBuilder builder,
                                                List<String> propertyKeys, Map<String, String> fieldNameMap) {
        for (String propertyKey : propertyKeys) {
            builder.literal(propertyKey);
            builder.field(fieldNameMap.get(propertyKey));
        }
    }

    /**
     * Responsible for building instances of {@link GraphSummarizer}
     */
    public static class GroupingBuilder extends TableGroupingBuilderBase {

        public GraphSummarizer build() {
            return new GraphSummarizer(
                    useVertexLabel,
                    useEdgeLabel,
                    vertexPropertyKeys,
                    vertexAggregateFunctions,
                    edgePropertyKeys,
                    edgeAggregateFunctions
            );
        }
    }
}
