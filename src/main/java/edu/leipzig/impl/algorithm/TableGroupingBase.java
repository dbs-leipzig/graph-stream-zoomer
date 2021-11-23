package edu.leipzig.impl.algorithm;

import edu.leipzig.impl.functions.aggregation.Count;
import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.impl.functions.utils.EmptyPropertyValue;
import edu.leipzig.impl.functions.utils.EmptyPropertyValueIfNull;
import edu.leipzig.impl.functions.utils.ExtractPropertyValue;
import edu.leipzig.impl.functions.utils.CreateSuperElementId;
import edu.leipzig.impl.functions.utils.PlannerExpressionBuilder;
import edu.leipzig.impl.functions.utils.PlannerExpressionSeqBuilder;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.table.TableSet;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static edu.leipzig.model.table.TableSet.*;
import static org.apache.flink.table.api.Expressions.*;

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

public abstract class TableGroupingBase {
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
     * Table set of the original stream graph
     */
    TableSet tableSet;
    /**
     * Stream Graph Configuration
     */
    StreamGraphConfig config;

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
     * Get the config.
     *
     * @return the stream graph config
     */
    public StreamGraphConfig getConfig() {
        return config;
    }

    /**
     * Get the table environment from the config.
     *
     * @return the table environment from the config
     */
    public StreamTableEnvironment getTableEnv() {
        return getConfig().getTableEnvironment();
    }

    /**
     * Collects all field names the vertex table gets grouped by
     * <p>
     * { property_1, property_2, ..., property_n, vertex_label }
     *
     * @return scala sequence of expressions
     */
    Expression[] buildVertexGroupExpressions() {

        List<Expression> expressions = new ArrayList<>();

        // tmp_a1, ... , tmp_an
        for (String vertexPropertyKey : vertexGroupingPropertyKeys) {
            expressions.add($(vertexGroupingPropertyFieldNames.get(vertexPropertyKey)));
        }

        if (useVertexLabels) {
            // optional: vertex_label
            expressions.add($(FIELD_VERTEX_LABEL));
        }
        else if (vertexGroupingPropertyKeys.size() == 0) {
            // in case no vertex criteria grouping specified
            expressions.add($(TableSet.FIELD_VERTEX_ID));
        }

        expressions.add($("eventWindow"));

        return expressions.toArray(new Expression[0]);
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
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(getTableEnv());
        String[] fields;
        // computes super_vertex_id as the hash value of vertex grouping fields values
        if (useVertexLabels && vertexGroupingPropertyKeys.size() > 0) {
            fields = new String[vertexGroupingPropertyFieldNames.size() + 1];
            fields[0] = FIELD_VERTEX_LABEL;
            for (int i = 1; i <= vertexGroupingPropertyFieldNames.size(); i++)
                fields[i] = vertexGroupingPropertyFieldNames.get(vertexGroupingPropertyKeys.get(i - 1));

        } else if (useVertexLabels) {
            fields = new String[1];
            fields[0] = FIELD_VERTEX_LABEL;
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
          .scalarFunctionCall(new CreateSuperElementId(), fields)
          .as(FIELD_SUPER_VERTEX_ID);

        // optional: vertex_label
        if (useVertexLabels) {
            builder.field(FIELD_VERTEX_LABEL).as(FIELD_SUPER_VERTEX_LABEL);
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
            PlannerExpressionBuilder expressionToAggregateBuilder = new PlannerExpressionBuilder(getTableEnv());

            String fieldNameAfterAggregation = config.createUniqueAttributeName();
            vertexAfterAggregationPropertyFieldNames
              .put(vertexAggregationFunction.getAggregatePropertyKey(), fieldNameAfterAggregation);

            if (vertexAggregationFunction instanceof Count){
                // Use new expression api for count
                builder.count();

            } else if (null != vertexAggregationFunction.getPropertyKey()) {
                // Property aggregation function, e.g. MAX, MIN, SUM
                expressionToAggregateBuilder.field(vertexAggregationPropertyFieldNames
                        .get(vertexAggregationFunction.getAggregatePropertyKey()));

                builder
                  .aggFunctionCall(vertexAggregationFunction.getTableAggFunction(),
                    expressionToAggregateBuilder.getExpression());
            } else {
                throw new UnsupportedOperationException("Unable to handle aggregation function [" +
                  vertexAggregationFunction.getClass() + "].");
            }

            builder.as(fieldNameAfterAggregation);
        }
        return builder.build();
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
        ApiExpression joinPredicate = null;

        PlannerExpressionSeqBuilder preparedVerticesProjectExpressionsBuilder =
          new PlannerExpressionSeqBuilder(getTableEnv())
            .field(TableSet.FIELD_VERTEX_ID);
        PlannerExpressionSeqBuilder groupedVerticesProjectExpressionsBuilder =
          new PlannerExpressionSeqBuilder(getTableEnv())
            .field(FIELD_SUPER_VERTEX_ID);

        for (String vertexPropertyKey : vertexGroupingPropertyKeys) {
            String fieldNameBeforeGrouping = vertexGroupingPropertyFieldNames.get(vertexPropertyKey);
            preparedVerticesProjectExpressionsBuilder
              .scalarFunctionCall(new EmptyPropertyValueIfNull(), fieldNameBeforeGrouping)
              .as(fieldNameBeforeGrouping);

            String fieldNameAfterGrouping = vertexAfterGroupingPropertyFieldNames.get(vertexPropertyKey);
            groupedVerticesProjectExpressionsBuilder.field(fieldNameAfterGrouping);

            if (null == joinPredicate) {
                joinPredicate = $(fieldNameBeforeGrouping).isEqual($(fieldNameAfterGrouping));
            } else {
                joinPredicate = joinPredicate.and($(fieldNameBeforeGrouping).isEqual($(fieldNameAfterGrouping)));
            }
        }

        if (useVertexLabels) {
            preparedVerticesProjectExpressionsBuilder.field(FIELD_VERTEX_LABEL);
            groupedVerticesProjectExpressionsBuilder.field(FIELD_SUPER_VERTEX_LABEL);

            if (null == joinPredicate) {
                joinPredicate = $(FIELD_VERTEX_LABEL).isEqual($(FIELD_SUPER_VERTEX_LABEL));
            } else {
                joinPredicate = joinPredicate.and($(FIELD_VERTEX_LABEL).isEqual($(FIELD_SUPER_VERTEX_LABEL)));
            }
        }

        return preparedVertices
          .select(preparedVerticesProjectExpressionsBuilder.build())
          .join(
            groupedVertices.select(groupedVerticesProjectExpressionsBuilder.build()),
            joinPredicate)
          .select(new PlannerExpressionSeqBuilder(getTableEnv())
            .field(TableSet.FIELD_VERTEX_ID)
            .field(FIELD_SUPER_VERTEX_ID)
            .build());
    }

    /**
     * Assigns edges to super vertices by replacing source and target id with corresponding super vertex ids
     * <p>
     * π_{edge_id, new_source_id, new_target_id, edge_label}(
     * Edges ⋈_{target_id=vertex_id}(π_{vertex_id, super_vertex_id AS new_target_id}(ExpandedVertices))
     * ⋈_{source_id=vertex_id}(π_{vertex_id, super_vertex_id AS new_source_id}(ExpandedVertices))
     * )
     *
     * @param edges                        table of edges to assign super vertices to
     * @param expandedVertices             vertex - super vertex mapping table
     * @return enriched edges table
     */
    Table enrichEdges(Table edges, Table expandedVertices) {
        String vertexTargetId = config.createUniqueAttributeName();
        String superVertexTargetId = config.createUniqueAttributeName();
        String vertexTargetEventTime = config.createUniqueAttributeName();
        String vertexSourceId = config.createUniqueAttributeName();
        String superVertexSourceId = config.createUniqueAttributeName();
        String vertexSourceEventTime = config.createUniqueAttributeName();

        // 1. Set needed project expressions (edge_id, timestamp, source_id, target_id)
        PlannerExpressionSeqBuilder projectExpressionsBuilder = new PlannerExpressionSeqBuilder(getTableEnv())
          .field(TableSet.FIELD_EDGE_ID)
          .field(FIELD_EVENT_TIME)
          .field(superVertexSourceId).as(TableSet.FIELD_SOURCE_ID)
          .field(superVertexTargetId).as(TableSet.FIELD_TARGET_ID);

        // 2. optionally: edge_label
        if (useEdgeLabels) {
            projectExpressionsBuilder.field(TableSet.FIELD_EDGE_LABEL);
        }

        // 3. Add properties (key and aggregate)

        // grouping_property_1 AS tmp_e1, ... , grouping_property_k AS tmp_ek
        for (String propertyKey : edgeGroupingPropertyKeys) {
            String propertyColumnName = getConfig().createUniqueAttributeName();
            projectExpressionsBuilder
              .scalarFunctionCall(new ExtractPropertyValue(propertyKey),
                TableSet.FIELD_EDGE_PROPERTIES)
              .as(propertyColumnName);
            edgeGroupingPropertyFieldNames.put(propertyKey, propertyColumnName);
        }

        // property_to_aggregate_1 AS tmp_f1, ... , property_to_aggregate_l AS tmp_fl
        for (CustomizedAggregationFunction aggregateFunction : edgeAggregateFunctions) {
            if (null != aggregateFunction.getPropertyKey()) {
                String propertyColumnName = getConfig().createUniqueAttributeName();
                projectExpressionsBuilder
                  .scalarFunctionCall(new ExtractPropertyValue(aggregateFunction.getPropertyKey()),
                    TableSet.FIELD_EDGE_PROPERTIES)
                  .as(propertyColumnName);
                edgeAggregationPropertyFieldNames.put(aggregateFunction.getAggregatePropertyKey(),
                  propertyColumnName);
            }
        }

        Table enrichedEdges = edges
          //.select(TableSet.getEdgeProjectExpressionsWithCastedRowtime())
          .join(expandedVertices
            .select(new PlannerExpressionSeqBuilder(getTableEnv())
              .field(TableSet.FIELD_VERTEX_ID).as(vertexTargetId)
              .field(FIELD_SUPER_VERTEX_ID).as(superVertexTargetId)
              //.field(FIELD_VERTEX_EVENT_TIME).as(vertexTargetEventTime)
              .build()),
            // join condition
            $(FIELD_TARGET_ID).isEqual($(vertexTargetId))
              //.and($(FIELD_EVENT_TIME).between($(vertexTargetEventTime).minus(lit(4).hours()), $(vertexTargetEventTime)))
              .and($(FIELD_EVENT_TIME).between($(FIELD_EVENT_TIME).minus(lit(4).minutes()), $(FIELD_EVENT_TIME)))
          )
          .join(expandedVertices
            .select(new PlannerExpressionSeqBuilder(getTableEnv())
                .field(TableSet.FIELD_VERTEX_ID).as(vertexSourceId)
                .field(FIELD_SUPER_VERTEX_ID).as(superVertexSourceId)
              //  .field(FIELD_VERTEX_EVENT_TIME).as(vertexSourceEventTime)
              .build()),
            // join condition
            $(FIELD_SOURCE_ID).isEqual($(vertexSourceId))
          //    .and($(FIELD_EVENT_TIME).between($(vertexSourceEventTime).minus(lit(4).hours()), $(vertexSourceEventTime)))
              .and($(FIELD_EVENT_TIME).between($(FIELD_EVENT_TIME).minus(lit(4).minutes()), $(FIELD_EVENT_TIME)))
            );

        return enrichedEdges.select(projectExpressionsBuilder.build());
    }

    /**
     * Collects all field names the edge relation gets grouped by
     * <p>
     * { source_id, target_id, property_1, property_2, ..., property_k, edge_label }
     *
     * @return scala sequence of expressions
     */
    Expression[] buildEdgeGroupExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(getTableEnv());

        // source_id, target_id
        builder.field(TableSet.FIELD_SOURCE_ID);
        builder.field(TableSet.FIELD_TARGET_ID);

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

        //builder.field("eventWindow");

        return builder.build();
    }

    /**
     * Collects all expressions the grouped edge table gets projected to
     * <p>
     * { super_edge_id,
     * source_id,
     * target_id,
     * edge_label,
     * property_1, property_2, ..., property_k,
     * aggregate_1, ... aggregate_l }
     *
     * @return scala sequence of expressions
     */
    Expression[] buildEdgeProjectExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(getTableEnv());
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
                    .scalarFunctionCall(new CreateSuperElementId(), fields)
                    .as(FIELD_SUPER_EDGE_ID);
        } // no edge grouping criteria specified
        else {
            fields = new String[1];
            fields[0] = TableSet.FIELD_EDGE_ID;
        }
        builder
                .scalarFunctionCall(new CreateSuperElementId(), fields)
                .as(FIELD_SUPER_EDGE_ID);

        // source_id, target_id
        builder
                .field(TableSet.FIELD_SOURCE_ID)
                .field(TableSet.FIELD_TARGET_ID);

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
            PlannerExpressionBuilder expressionToAggregateBuilder = new PlannerExpressionBuilder(getTableEnv());
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
                            expressionToAggregateBuilder.getExpression())
                    .as(fieldNameAfterAggregation);

            edgeAfterAggregationPropertyFieldNames.put(edgeAggregateFunction.getAggregatePropertyKey(),
                    fieldNameAfterAggregation);
        }

        // handle timestamp
        //builder.expression($(FIELD_EVENT_TIME).max()).as(FIELD_EVENT_TIME);

        return builder.build();
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

    public static class GroupingBuilder {

        /**
         * Used as property key to declare a label based grouping.*
         */
        private static final String LABEL_SYMBOL = ":label";
        /**
         * List of property keys to group vertices by
         */
        final List<String> vertexPropertyKeys;
        /**
         * List of aggregate functions to execute on grouped vertices
         */
        final List<CustomizedAggregationFunction> vertexAggregateFunctions;
        /**
         * List of property keys to group edges by
         */
        final List<String> edgePropertyKeys;
        /**
         * List of aggregate functions to execute on grouped edges
         */
        final List<CustomizedAggregationFunction> edgeAggregateFunctions;
        /**
         * True, iff vertex labels shall be considered.
         */
        boolean useVertexLabel;
        /**
         * True, iff edge labels shall be considered.
         */
        boolean useEdgeLabel;

        /**
         * Creates a new grouping builder
         */
        public GroupingBuilder() {
            this.useVertexLabel = false;
            this.useEdgeLabel = false;
            this.vertexPropertyKeys = new ArrayList<>();
            this.vertexAggregateFunctions = new ArrayList<>();
            this.edgePropertyKeys = new ArrayList<>();
            this.edgeAggregateFunctions = new ArrayList<>();
        }

        /**
         * Adds a property key to the vertex grouping keys
         *
         * @param key property key
         * @return this builder
         */
        public GroupingBuilder addVertexGroupingKey(String key) {
            Objects.requireNonNull(key);
            if (key.equals(LABEL_SYMBOL)) {
                return useVertexLabel(true);
            } else {
                vertexPropertyKeys.add(key);
            }
            return this;
        }

        /**
         * Adds a list of property keys to the vertex grouping keys
         *
         * @param keys property keys
         * @return this builder
         */
        public GroupingBuilder addVertexGroupingKeys(List<String> keys) {
            Objects.requireNonNull(keys);
            for (String key : keys) {
                this.addVertexGroupingKey(key);
            }
            return this;
        }

        /**
         * Adds a property key to the edge grouping keys
         *
         * @param key property key
         * @return this builder
         */
        public GroupingBuilder addEdgeGroupingKey(String key) {
            Objects.requireNonNull(key);
            if (key.equals(LABEL_SYMBOL)) {
                return useEdgeLabel(true);
            } else {
                edgePropertyKeys.add(key);
            }
            return this;
        }

        /**
         * Adds a list of property keys to the edge grouping keys
         *
         * @param keys property keys
         * @return this builder
         */
        public GroupingBuilder addEdgeGroupingKeys(List<String> keys) {
            Objects.requireNonNull(keys);
            for (String key : keys) {
                this.addEdgeGroupingKey(key);
            }
            return this;
        }

        /**
         * Add an aggregate function which is applied on all vertices represented by a single super
         * vertex
         *
         * @param aggregateFunction vertex aggregate function
         * @return this builder
         */
        public GroupingBuilder addVertexAggregateFunction(CustomizedAggregationFunction aggregateFunction) {
            Objects.requireNonNull(aggregateFunction, "Aggregate function must not be null");
            vertexAggregateFunctions.add(aggregateFunction);
            return this;
        }

        /**
         * Add an aggregate function which is applied on all edges represented by a single super edge
         *
         * @param aggregateFunction edge aggregate function
         * @return this builder
         */
        public GroupingBuilder addEdgeAggregateFunction(CustomizedAggregationFunction aggregateFunction) {
            Objects.requireNonNull(aggregateFunction, "Aggregate function must not be null");
            edgeAggregateFunctions.add(aggregateFunction);
            return this;
        }

        /**
         * Define, if the vertex label shall be used for grouping vertices.
         *
         * @param useVertexLabel true, iff vertex label shall be used for grouping
         * @return this builder
         */
        private GroupingBuilder useVertexLabel(boolean useVertexLabel) {
            this.useVertexLabel = useVertexLabel;
            return this;
        }

        /**
         * Define, if the edge label shall be used for grouping edges.
         *
         * @param useEdgeLabel true, iff edge label shall be used for grouping
         * @return this builder
         */
        private GroupingBuilder useEdgeLabel(boolean useEdgeLabel) {
            this.useEdgeLabel = useEdgeLabel;
            return this;
        }

        /**
         * Build grouping operator instance
         *
         * @return grouping operator instance
         */
        public GraphStreamGrouping build() {
            return new GraphStreamGrouping(useVertexLabel, useEdgeLabel, vertexPropertyKeys,
              vertexAggregateFunctions, edgePropertyKeys, edgeAggregateFunctions);
        }
    }
}
