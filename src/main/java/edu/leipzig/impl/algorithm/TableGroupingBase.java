package edu.leipzig.impl.algorithm;

import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.impl.functions.utils.*;
import edu.leipzig.model.streamGraph.StreamGraphConfig;
import edu.leipzig.model.table.TableSet;
import edu.leipzig.model.table.TableSetFactory;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.planner.expressions.PlannerExpression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class for table based grouping implementations which are built upon a table set which
 * extends the table set (there need to be at least two tables: vertices and edges)
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has some changes.
 * these changes is related to using this operator on streaming api.
 *
 * @link TableGroupingBase
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.operators.grouping;
 */

abstract class TableGroupingBase {
    /**
     * Field name for super vertex id
     */
    static final String FIELD_SUPER_VERTEX_ID = "super_vertex_id";
    /**
     * Field name for super vertex label
     */
    static final String FIELD_SUPER_VERTEX_LABEL = "super_vertex_label";
    /**
     * Field name for super edge
     */
    static final String FIELD_SUPER_EDGE_ID = "super_edge_id";
    /**
     * True if vertices shall be grouped using their label.
     */
    final boolean useVertexLabels;
    /**
     * True if edges shall be grouped using their label.
     */
    final boolean useEdgeLabels;
    /**
     * List of property keys to group vertices by (property_1, ..., property_n)
     */
    final List<String> vertexGroupingPropertyKeys;
    /**
     * List of aggregate functions to execute on grouped vertices
     */
    final List<CustomizedAggregationFunction> vertexAggregateFunctions;
    /**
     * List of property keys to group edges by
     */
    final List<String> edgeGroupingPropertyKeys;
    /**
     * List of aggregate functions to execute on grouped edges
     */
    final List<CustomizedAggregationFunction> edgeAggregateFunctions;
    /**
     * Table set factory
     */
    TableSetFactory tableSetFactory;
    /**
     * Table set of the original stream graph
     */
    TableSet tableSet;
    /**
     * Stream Graph Configuration
     */
    StreamGraphConfig config;
    /**
     * Flink Table Environment
     */
    StreamTableEnvironment tableEnv;
    /**
     * Helper for building flink table expressions
     */
    PlannerExpressionBuilder builder;
    /**
     * Field names for properties to group vertices by, i. e. a mapping
     * <p>
     * property_1 -> tmp_a1
     * ...
     * property_n -> tmp_an
     */
    Map<String, String> vertexGroupingPropertyFieldNames;

    /**
     * Field names for vertex grouping properties after grouping, i. e. a mapping
     * <p>
     * property_1 -> tmp_c1
     * ...
     * property_n -> tmp_cn
     */
    Map<String, String> vertexAfterGroupingPropertyFieldNames;

    /**
     * Field names for properties to group edges by, i. e. a mapping
     * <p>
     * property_1 -> tmp_e1
     * ...
     * property_k -> tmp_ek
     */
    Map<String, String> edgeGroupingPropertyFieldNames;

    /**
     * Field names for edge grouping properties after grouping, i. e. a mapping
     * <p>
     * property_1 -> tmp_g1
     * ...
     * property_k -> tmp_gk
     */
    Map<String, String> edgeAfterGroupingPropertyFieldNames;

    /**
     * Field names for vertex properties to aggregate, i. e. a mapping
     * <p>
     * property_1 -> tmp_b1
     * ...
     * property_m -> tmp_bm
     */
    Map<String, String> vertexAggregationPropertyFieldNames;

    /**
     * Field names for vertex aggregation properties after aggregation, i. e. a mapping
     * <p>
     * property_1 -> tmp_d1
     * ...
     * property_m -> tmp_dm
     */
    Map<String, String> vertexAfterAggregationPropertyFieldNames;

    /**
     * Field names for edge properties to aggregate, i. e. a mapping
     * <p>
     * property_1 -> tmp_f1
     * ...
     * property_l -> tmp_fl
     */
    Map<String, String> edgeAggregationPropertyFieldNames;

    /**
     * Field names for edge aggregation properties after aggregation, i. e. a mapping
     * <p>
     * property_1 -> tmp_h1
     * ...
     * property_l -> tmp_hl
     */
    Map<String, String> edgeAfterAggregationPropertyFieldNames;

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
    TableGroupingBase(
            boolean useVertexLabels,
            boolean useEdgeLabels,
            List<String> vertexGroupingPropertyKeys,
            List<CustomizedAggregationFunction> vertexAggregateFunctions,
            List<String> edgeGroupingPropertyKeys,
            List<CustomizedAggregationFunction> edgeAggregateFunctions
    ) {
        this.useVertexLabels = useVertexLabels;
        this.useEdgeLabels = useEdgeLabels;
        this.vertexGroupingPropertyKeys = vertexGroupingPropertyKeys;
        this.vertexAggregateFunctions = vertexAggregateFunctions;
        this.edgeGroupingPropertyKeys = edgeGroupingPropertyKeys;
        this.edgeAggregateFunctions = edgeAggregateFunctions;
        this.vertexGroupingPropertyFieldNames = new HashMap<>();
        this.vertexAfterGroupingPropertyFieldNames = new HashMap<>();
        this.edgeGroupingPropertyFieldNames = new HashMap<>();
        this.edgeAfterGroupingPropertyFieldNames = new HashMap<>();
        this.vertexAggregationPropertyFieldNames = new HashMap<>();
        this.vertexAfterAggregationPropertyFieldNames = new HashMap<>();
        this.edgeAggregationPropertyFieldNames = new HashMap<>();
        this.edgeAfterAggregationPropertyFieldNames = new HashMap<>();
    }

    /**
     * Perform grouping based on {@link this#tableSet} and put result tables into new table set
     *
     * @return table set of result stream graph
     */
    abstract TableSet performGrouping();

    /**
     * Collects all field names the vertex table gets grouped by
     * <p>
     * { property_1, property_2, ..., property_n, vertex_label }
     *
     * @return scala sequence of expressions
     */
    Expression[] buildVertexGroupExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder();

        // tmp_a1, ... , tmp_an
        for (String vertexPropertyKey : vertexGroupingPropertyKeys) {
            builder.field(vertexGroupingPropertyFieldNames.get(vertexPropertyKey));
        }

        // optional: vertex_label
        if (useVertexLabels) {
            builder.field(TableSet.FIELD_VERTEX_LABEL);
        } // in case no vertex criteria grouping specified
        else if (vertexGroupingPropertyKeys.size() == 0) {
            builder.field(TableSet.FIELD_VERTEX_ID);
        }
        // return (Expression[]) builder.buildList().toArray();
        return builder.buildArray();
    }

    /**
     * Collects all expressions the grouped vertex table gets projected to
     * <p>
     * { super_vertex_id,
     * vertex_label,
     * property_1, property_2, ..., property_n,
     * aggregate_1, ... aggregate_m }
     *
     * @return scala sequence of expressions
     */
    Expression[] buildVertexProjectExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder();
        String[] fields;
        // computes super_vertex_id as the hash value of vertex grouping fields values
        if (useVertexLabels && vertexGroupingPropertyKeys.size() > 0) {
            fields = new String[vertexGroupingPropertyFieldNames.size() + 1];
            fields[0] = TableSet.FIELD_VERTEX_LABEL;
            for (int i = 1; i <= vertexGroupingPropertyFieldNames.size(); i++)
                fields[i] = vertexGroupingPropertyFieldNames.get(vertexGroupingPropertyKeys.get(i - 1));

        } else if (useVertexLabels) {
            fields = new String[1];
            fields[0] = TableSet.FIELD_VERTEX_LABEL;
        } else if (vertexGroupingPropertyKeys.size() > 0) {
            fields = new String[vertexGroupingPropertyFieldNames.size()];
            for (int i = 0; i < vertexGroupingPropertyFieldNames.size(); i++)
                fields[i] = vertexGroupingPropertyFieldNames.get(vertexGroupingPropertyKeys.get(i));
        } // no vertex grouping criteria specified
        else {
            fields = new String[1];
            fields[0] = TableSet.FIELD_VERTEX_ID;
        }
        builder
                .scalarFunctionCall(new NewGroupingKey(),
                        fields)
                .as(FIELD_SUPER_VERTEX_ID);

        // optional: vertex_label
        if (useVertexLabels) {
            builder.field(TableSet.FIELD_VERTEX_LABEL).as(FIELD_SUPER_VERTEX_LABEL);
        }

        // tmp_a1 AS tmp_c1, ... , tmp_an AS tmp_cn
        for (String vertexPropertyKey : vertexGroupingPropertyKeys) {
            String fieldNameBeforeGrouping = vertexGroupingPropertyFieldNames.get(vertexPropertyKey);
            String fieldNameAfterGrouping = config.createUniqueAttributeName();
            builder
                    .scalarFunctionCall(new EmptyPropertyValueIfNull(), fieldNameBeforeGrouping)
                    .as(fieldNameAfterGrouping);
            vertexAfterGroupingPropertyFieldNames.put(vertexPropertyKey, fieldNameAfterGrouping);
        }

        // AGG(tmp_b1) AS tmp_d1, ... , AGG(tmp_bm) AS tmp_dm
        for (CustomizedAggregationFunction vertexAggregationFunction : vertexAggregateFunctions) {
            PlannerExpressionBuilder expressionToAggregateBuilder = new PlannerExpressionBuilder();

            if (null != vertexAggregationFunction.getPropertyKey()) {
                // Property aggregation function, e.g. MAX, MIN, SUM
                expressionToAggregateBuilder.field(vertexAggregationPropertyFieldNames
                        .get(vertexAggregationFunction.getAggregatePropertyKey()));
            } else {
                // Non-property aggregation function, e.g. COUNT
                expressionToAggregateBuilder.scalarFunctionCall(new EmptyPropertyValue());
            }

            String fieldNameAfterAggregation = config.createUniqueAttributeName();
            builder
                    .aggFunctionCall(vertexAggregationFunction.getTableAggFunction(),
                            new PlannerExpression[]{expressionToAggregateBuilder.toExpression()})
                    .as(fieldNameAfterAggregation);

            vertexAfterAggregationPropertyFieldNames
                    .put(vertexAggregationFunction.getAggregatePropertyKey(), fieldNameAfterAggregation);
        }
        // return (Expression[]) builder.buildList().toArray();
        return builder.buildArray();
    }

    /**
     * Joins the original vertex set with grouped vertex set in order to get a
     * vertex_id -> super_vertex_id mapping
     * <p>
     * π_{vertex_id, super_vertex_id}(
     * π_{vertex_id, vertex_label, property_a1, ..., property_an}(PreparedVertices) ⋈_{
     * vertex_label = super_vertex_label, property_a1 = property_c1, ... property_an =property_cn
     * } (
     * π_{super_vertex_id, super_vertex_label, property_c1, ..., property_cn}(GroupedVertices)
     * )
     * )
     *
     * @param preparedVertices original prepared vertex table
     * @param groupedVertices  grouped vertex table
     * @return vertex - super vertex mapping table
     */
    Table joinVerticesWithGroupedVertices(Table preparedVertices, Table groupedVertices) {
        PlannerExpressionBuilder joinPredicateBuilder = new PlannerExpressionBuilder();

        PlannerExpressionSeqBuilder preparedVerticesProjectExpressionsBuilder = new PlannerExpressionSeqBuilder()
                .field(TableSet.FIELD_VERTEX_ID);
        PlannerExpressionSeqBuilder groupedVerticesProjectExpressionsBuilder = new PlannerExpressionSeqBuilder()
                .field(FIELD_SUPER_VERTEX_ID);

        for (String vertexPropertyKey : vertexGroupingPropertyKeys) {
            String fieldNameBeforeGrouping = vertexGroupingPropertyFieldNames.get(vertexPropertyKey);
            preparedVerticesProjectExpressionsBuilder
                    .scalarFunctionCall(new EmptyPropertyValueIfNull(), fieldNameBeforeGrouping)
                    .as(fieldNameBeforeGrouping);

            String fieldNameAfterGrouping = vertexAfterGroupingPropertyFieldNames.get(vertexPropertyKey);
            groupedVerticesProjectExpressionsBuilder.field(fieldNameAfterGrouping);

            joinPredicateBuilder.and(
                    new PlannerExpressionBuilder()
                            .field(fieldNameBeforeGrouping)
                            .equalTo(fieldNameAfterGrouping)
                            .toExpression()
            );

        }

        if (useVertexLabels) {
            preparedVerticesProjectExpressionsBuilder.field(TableSet.FIELD_VERTEX_LABEL);
            groupedVerticesProjectExpressionsBuilder.field(FIELD_SUPER_VERTEX_LABEL);

            joinPredicateBuilder.and(
                    new PlannerExpressionBuilder()
                            .field(TableSet.FIELD_VERTEX_LABEL)
                            .equalTo(FIELD_SUPER_VERTEX_LABEL)
                            .toExpression()
            );
        }

        return preparedVertices.
                select(preparedVerticesProjectExpressionsBuilder.buildArray())
                .join(groupedVertices.select(groupedVerticesProjectExpressionsBuilder.buildArray()),
                        joinPredicateBuilder.toExpression())
                .select(new PlannerExpressionSeqBuilder()
                        .field(TableSet.FIELD_VERTEX_ID)
                        .field(FIELD_SUPER_VERTEX_ID)
                        .buildArray()
                );
    }

    /**
     * Assigns edges to super vertices by replacing head and tail id with corresponding super
     * vertex ids
     * <p>
     * π_{edge_id, new_tail_id, new_head_id, edge_label}(
     * Edges ⋈_{head_id=vertex_id}(π_{vertex_id, super_vertex_id AS new_head_id}(ExpandedVertices))
     * ⋈_{tail_id=vertex_id}(π_{vertex_id, super_vertex_id AS new_tail_id}(ExpandedVertices))
     * )
     *
     * @param edges                        table of edges to assign super vertices to
     * @param expandedVertices             vertex - super vertex mapping table
     * @param additionalProjectExpressions additional expressions the edges get projected to
     * @return enriched edges table
     */
    Table enrichEdges(Table edges, Table expandedVertices,
                      PlannerExpression... additionalProjectExpressions) {

        String vertexIdHead = config.createUniqueAttributeName();
        String superVertexIdHead = config.createUniqueAttributeName();
        String vertexIdTail = config.createUniqueAttributeName();
        String superVertexIdTail = config.createUniqueAttributeName();

        PlannerExpressionSeqBuilder projectExpressionsBuilder = new PlannerExpressionSeqBuilder()
                .field(TableSet.FIELD_EDGE_ID)
                .field(superVertexIdTail).as(TableSet.FIELD_TAIL_ID)
                .field(superVertexIdHead).as(TableSet.FIELD_HEAD_ID);

        // optionally: edge_label
        if (useEdgeLabels) {
            projectExpressionsBuilder.field(TableSet.FIELD_EDGE_LABEL);
        }

        for (PlannerExpression expression : additionalProjectExpressions) {
            projectExpressionsBuilder.expression(expression);
        }

        return tableSet.getEdges()
                .join(expandedVertices
                        .select(new PlannerExpressionSeqBuilder()
                                .field(TableSet.FIELD_VERTEX_ID).as(vertexIdHead)
                                .field(FIELD_SUPER_VERTEX_ID).as(superVertexIdHead)
                                // .buildList().toArray()
                                .buildArray()
                        ), new PlannerExpressionBuilder()
                        .field(TableSet.FIELD_HEAD_ID).equalTo(vertexIdHead).toExpression()
                )
                .join(expandedVertices
                        .select(new PlannerExpressionSeqBuilder()
                                .field(TableSet.FIELD_VERTEX_ID).as(vertexIdTail)
                                .field(FIELD_SUPER_VERTEX_ID).as(superVertexIdTail)
                                // .buildList().toArray()
                                .buildArray()
                        ), new PlannerExpressionBuilder()
                        .field(TableSet.FIELD_TAIL_ID).equalTo(vertexIdTail).toExpression()
                )
                .select(projectExpressionsBuilder.buildArray()
                );
    }

    /**
     * Collects all field names the edge relation gets grouped by
     * <p>
     * { tail_id, head_id, property_1, property_2, ..., property_k, edge_label }
     *
     * @return scala sequence of expressions
     */
    Expression[] buildEdgeGroupExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder();

        // tail_id, head_id
        builder.field(TableSet.FIELD_TAIL_ID);
        builder.field(TableSet.FIELD_HEAD_ID);

        // tmp_e1, ... , tmp_ek
        for (String edgePropertyKey : edgeGroupingPropertyKeys) {
            builder.field(edgeGroupingPropertyFieldNames.get(edgePropertyKey));
        }

        // optionally: edge_label
        if (useEdgeLabels) {
            builder.field(TableSet.FIELD_EDGE_LABEL);
        } // in case no edge criteria grouping specified
        else if (edgeGroupingPropertyKeys.size() == 0) {
            builder.field(TableSet.FIELD_EDGE_ID);
        }

        // return (Expression[]) builder.buildList().toArray();
        return builder.buildArray();
    }

    /**
     * Collects all expressions the grouped edge table gets projected to
     * <p>
     * { super_edge_id,
     * tail_id,
     * head_id,
     * edge_label,
     * property_1, property_2, ..., property_k,
     * aggregate_1, ... aggregate_l }
     *
     * @return scala sequence of expressions
     */
    Expression[] buildEdgeProjectExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder();
        String[] fields;
        // computes super_edge_id as the hash value of edge grouping fields values
        if (useEdgeLabels && edgeGroupingPropertyKeys.size() > 0) {
            fields = new String[edgeGroupingPropertyFieldNames.size() + 1];
            fields[0] = TableSet.FIELD_EDGE_LABEL;
            for (int i = 1; i <= edgeGroupingPropertyKeys.size(); i++)
                fields[i] = edgeGroupingPropertyFieldNames.get(edgeGroupingPropertyKeys.get(i - 1));

        } else if (useEdgeLabels) {
            fields = new String[1];
            fields[0] = TableSet.FIELD_EDGE_LABEL;
        } else if (edgeGroupingPropertyKeys.size() > 0) {
            fields = new String[edgeGroupingPropertyFieldNames.size()];
            for (int i = 0; i < edgeGroupingPropertyKeys.size(); i++)
                fields[i] = edgeGroupingPropertyFieldNames.get(edgeGroupingPropertyKeys.get(i));
            builder
                    .scalarFunctionCall(new NewGroupingKey(),
                            fields)
                    .as(FIELD_SUPER_EDGE_ID);
        } // no edge grouping criteria specified
        else {
            fields = new String[1];
            fields[0] = TableSet.FIELD_EDGE_ID;
        }
        builder
                .scalarFunctionCall(new NewGroupingKey(),
                        fields)
                .as(FIELD_SUPER_EDGE_ID);

        // tail_head, head_id
        builder
                .field(TableSet.FIELD_TAIL_ID)
                .field(TableSet.FIELD_HEAD_ID);

        // optional: edge_label
        if (useEdgeLabels) {
            builder.field(TableSet.FIELD_EDGE_LABEL);
        }

        // tmp_e1 AS tmp_g1, ... , tmp_ek AS tmp_gk
        for (String edgePropertyKey : edgeGroupingPropertyKeys) {
            String fieldNameBeforeGrouping = edgeGroupingPropertyFieldNames.get(edgePropertyKey);
            String fieldNameAfterGrouping = config.createUniqueAttributeName();
            builder
                    .scalarFunctionCall(new EmptyPropertyValueIfNull(), fieldNameBeforeGrouping)
                    .as(fieldNameAfterGrouping);
            edgeAfterGroupingPropertyFieldNames.put(edgePropertyKey, fieldNameAfterGrouping);
        }

        // AGG(tmp_f1) AS tmp_h1, ... , AGG(tmp_fl) AS tmp_hl
        for (CustomizedAggregationFunction edgeAggregateFunction : edgeAggregateFunctions) {
            PlannerExpressionBuilder expressionToAggregateBuilder = new PlannerExpressionBuilder();
            if (null != edgeAggregateFunction.getPropertyKey()) {
                // Property aggregation function, e.g. MAX, MIN, SUM
                expressionToAggregateBuilder
                        .field(edgeAggregationPropertyFieldNames
                                .get(edgeAggregateFunction.getAggregatePropertyKey()));
            } else {
                // Non-property aggregation function, e.g. COUNT
                expressionToAggregateBuilder.scalarFunctionCall(new EmptyPropertyValue());
            }

            String fieldNameAfterAggregation = config.createUniqueAttributeName();
            builder
                    .aggFunctionCall(edgeAggregateFunction.getTableAggFunction(),
                            new PlannerExpression[]{expressionToAggregateBuilder.toExpression()})
                    .as(fieldNameAfterAggregation);

            edgeAfterAggregationPropertyFieldNames.put(edgeAggregateFunction.getAggregatePropertyKey(),
                    fieldNameAfterAggregation);
        }

        // return (Expression[]) builder.buildList().toArray();
        return builder.buildArray();
    }

    /**
     * Returns a list of all super vertex property keys which will hold an aggregate after vertex
     * grouping
     *
     * @return list of all super vertex property keys which will hold an aggregate after vertex
     */
    List<String> getVertexAggregatedPropertyKeys() {
        return getAggregatedPropertyKeys(vertexAggregateFunctions);
    }

    /**
     * Returns a list of all super edge property keys which will hold an aggregate after edge grouping
     *
     * @return list of all super edge property keys which will hold an aggregate after edge grouping
     */
    List<String> getEdgeAggregatedPropertyKeys() {
        return getAggregatedPropertyKeys(edgeAggregateFunctions);
    }

    /**
     * Returns a list of all property keys which will hold an aggregate after grouping for given
     * list of aggregate functions
     *
     * @param functions list of aggregate functions
     * @return list of all property keys which will hold an aggregate after grouping
     */
    private List<String> getAggregatedPropertyKeys(List<CustomizedAggregationFunction> functions) {
        return functions.stream()
                .map(CustomizedAggregationFunction::getAggregatePropertyKey)
                .collect(Collectors.toList());
    }
}
