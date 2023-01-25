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
package edu.dbsleipzig.stream.grouping.impl.algorithm;

import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.Count;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.EmptyProperties;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.EmptyPropertyValue;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.EmptyPropertyValueIfNull;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.ExtractPropertyValue;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.CreateSuperElementId;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.PlannerExpressionBuilder;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.PlannerExpressionSeqBuilder;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.ToProperties;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.WindowConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import edu.dbsleipzig.stream.grouping.model.table.TableSet;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static edu.dbsleipzig.stream.grouping.model.table.TableSet.*;
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

    static final String FIELD_SUPER_VERTEX_EVENT_WINDOW = "super_vertex_event_window";

    static final String FIELD_SUPER_VERTEX_ROWTIME = "super_vertex_rowtime";

    static final String FIELD_EDGE_EVENT_WINDOW = "edge_event_window";
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

    final WindowConfig windowConfig;

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
     * @param windowConfig               window configuration
     */
    TableGroupingBase(
      boolean useVertexLabels,
      boolean useEdgeLabels,
      List<String> vertexGroupingPropertyKeys,
      List<CustomizedAggregationFunction> vertexAggregateFunctions,
      List<String> edgeGroupingPropertyKeys,
      List<CustomizedAggregationFunction> edgeAggregateFunctions, WindowConfig windowConfig) {
        this.useVertexLabels = useVertexLabels;
        this.useEdgeLabels = useEdgeLabels;
        this.vertexGroupingPropertyKeys = vertexGroupingPropertyKeys;
        this.vertexAggregateFunctions = vertexAggregateFunctions;
        this.edgeGroupingPropertyKeys = edgeGroupingPropertyKeys;
        this.edgeAggregateFunctions = edgeAggregateFunctions;
        this.windowConfig = windowConfig;
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

        expressions.add($(FIELD_SUPER_VERTEX_EVENT_WINDOW));

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
        Expression[] fields;
        // computes super_vertex_id as the hash value of vertex grouping fields values
        builder.expression($(FIELD_SUPER_VERTEX_EVENT_WINDOW).rowtime().as(FIELD_SUPER_VERTEX_ROWTIME));
        if (useVertexLabels && vertexGroupingPropertyKeys.size() > 0) {
            fields = new Expression[vertexGroupingPropertyFieldNames.size() + 2];
            fields[0] = $(FIELD_VERTEX_LABEL);
            for (int i = 1; i <= vertexGroupingPropertyFieldNames.size(); i++)
                fields[i] = $(vertexGroupingPropertyFieldNames.get(vertexGroupingPropertyKeys.get(i - 1)));

        } else if (useVertexLabels) {
            fields = new Expression[2];
            fields[0] = $(FIELD_VERTEX_LABEL);
        } else if (vertexGroupingPropertyKeys.size() > 0) {
            fields = new Expression[vertexGroupingPropertyFieldNames.size() + 1];
            for (int i = 0; i < vertexGroupingPropertyFieldNames.size(); i++)
                fields[i] = $(vertexGroupingPropertyFieldNames.get(vertexGroupingPropertyKeys.get(i)));
        } // no vertex grouping criteria specified
        else {
            fields = new Expression[2];
            fields[0] = $(TableSet.FIELD_VERTEX_ID);
        }
        fields[fields.length-1] = $(FIELD_SUPER_VERTEX_EVENT_WINDOW).rowtime();
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


    Expression[] buildEdgeGroupProjectExpressions() {
        // 1. Set needed project expressions (edge_id, timestamp, source_id, target_id)
        PlannerExpressionSeqBuilder projectExpressionsBuilder = new PlannerExpressionSeqBuilder(getTableEnv())
          .field(FIELD_EDGE_ID)
          .field(FIELD_EVENT_TIME)
          .field(FIELD_SOURCE_ID)
          .field(FIELD_TARGET_ID);

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

        return projectExpressionsBuilder.build();
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

        builder.field(FIELD_EDGE_EVENT_WINDOW);

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
        Expression[] fields;
        // computes super_edge_id as the hash value of edge grouping fields values
        if (useEdgeLabels && edgeGroupingPropertyKeys.size() > 0) {
            fields = new Expression[edgeGroupingPropertyFieldNames.size() + 2];
            fields[0] = $(TableSet.FIELD_EDGE_LABEL);
            for (int i = 1; i <= edgeGroupingPropertyKeys.size(); i++)
                fields[i] = $(edgeGroupingPropertyFieldNames.get(edgeGroupingPropertyKeys.get(i - 1)));

        } else if (useEdgeLabels) {
            fields = new Expression[2];
            fields[0] = $(TableSet.FIELD_EDGE_LABEL);
        } else if (edgeGroupingPropertyKeys.size() > 0) {
            fields = new Expression[edgeGroupingPropertyFieldNames.size()];
            for (int i = 0; i < edgeGroupingPropertyKeys.size(); i++)
                fields[i] = $(edgeGroupingPropertyFieldNames.get(edgeGroupingPropertyKeys.get(i)));
        } // no edge grouping criteria specified
        else {
            fields = new Expression[2];
            fields[0] = $(TableSet.FIELD_EDGE_ID);
        }
        fields[fields.length-1] = $(FIELD_EDGE_EVENT_WINDOW).rowtime();
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
        builder.expression($(FIELD_EDGE_EVENT_WINDOW).rowtime()).as(FIELD_EVENT_TIME);

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

    /**
     * Builds expression to select necessary columns from expandedVertices table
     *
     * @return scala sequence of expressions defining the selected columns
     */
    protected Expression[] buildSelectFromExpandedVertices() {
        PlannerExpressionSeqBuilder selectFromExpandedVertices =
          new PlannerExpressionSeqBuilder(getTableEnv());
        selectFromExpandedVertices.field(FIELD_VERTEX_ID);
        selectFromExpandedVertices.field("preparedVerticesTime").as(FIELD_EVENT_TIME);
        selectFromExpandedVertices.field(FIELD_SUPER_VERTEX_ID);
        if (useVertexLabels) {
            selectFromExpandedVertices.field(FIELD_SUPER_VERTEX_LABEL);
        }
        return selectFromExpandedVertices.build();
    }
    /**
     * selects attributes from furtherPreparedVertices table to join them with groupedVertices table
     *
     * @return scala sequence of expressions defining the selected columns for the join
     */
    protected Expression[] buildSelectPreparedVerticesGroupAttributes(){
        PlannerExpressionSeqBuilder selectPreparedVerticesGroupAttributes =
          new PlannerExpressionSeqBuilder(getTableEnv());

        for (String key : vertexGroupingPropertyFieldNames.keySet()) {
            selectPreparedVerticesGroupAttributes.field(vertexGroupingPropertyFieldNames.get(key));
        }
        if (useVertexLabels) {
            selectPreparedVerticesGroupAttributes.field(FIELD_VERTEX_LABEL);
        }

        selectPreparedVerticesGroupAttributes.field(FIELD_VERTEX_EVENT_TIME).as("preparedVerticesTime");
        selectPreparedVerticesGroupAttributes.field(FIELD_VERTEX_ID);
        return selectPreparedVerticesGroupAttributes.build();
    }

    /**
     * selects attributes from groupedVertices table to join them with furtherPreparedVertices table
     *
     * @return scala sequence of expressions defining the selected columns for the join
     */
    protected Expression[] buildSelectGroupedVerticesGroupAttributes(){
        PlannerExpressionSeqBuilder selectGroupedVerticesGroupAttributes =
          new PlannerExpressionSeqBuilder(getTableEnv());

        for (String key : vertexGroupingPropertyFieldNames.keySet()) {
            selectGroupedVerticesGroupAttributes.field(vertexAfterGroupingPropertyFieldNames.get(key));
        }
        if (useVertexLabels) {
            selectGroupedVerticesGroupAttributes.field(FIELD_SUPER_VERTEX_LABEL);
        }
        selectGroupedVerticesGroupAttributes.field(FIELD_SUPER_VERTEX_ID);
        selectGroupedVerticesGroupAttributes.field(FIELD_SUPER_VERTEX_ROWTIME);
        return selectGroupedVerticesGroupAttributes.build();
    }

    /**
     * Defines the join condition for groupedVertices and furtherPreparedVertices to create the
     * expandedVertices table
     *
     * @return Join conditions connected via conjunctions
     */
    protected Expression buildJoinConditionForExpandedVertices(){
        PlannerExpressionSeqBuilder attributeJoinConditions = new PlannerExpressionSeqBuilder(getTableEnv());
        PlannerExpressionSeqBuilder temporalJoinConditions = new PlannerExpressionSeqBuilder(getTableEnv());
        for (String key : vertexGroupingPropertyFieldNames.keySet()) {
            attributeJoinConditions.expression($(vertexGroupingPropertyFieldNames.get(key))
              .isEqual($(vertexAfterGroupingPropertyFieldNames.get(key))));
            attributeJoinConditions.expression($(vertexGroupingPropertyFieldNames.get(key)).isNull()
              .and($(vertexAfterGroupingPropertyFieldNames.get(key)).isNull()));
        }
        if (useVertexLabels) {
            attributeJoinConditions.expression($(FIELD_VERTEX_LABEL).isEqual($(FIELD_SUPER_VERTEX_LABEL)));
            attributeJoinConditions.expression($(FIELD_VERTEX_LABEL).isNull().and($(FIELD_SUPER_VERTEX_LABEL)
              .isNull()));
        }
        temporalJoinConditions.expression($("preparedVerticesTime").isLessOrEqual($(FIELD_SUPER_VERTEX_ROWTIME)))
          .and($("preparedVerticesTime").isGreater($(FIELD_SUPER_VERTEX_ROWTIME).minus(windowConfig.getWindowExpression())));
        Expression[] joinConditionArray = attributeJoinConditions.build();
        ArrayList<ApiExpression> orConnectedConditions = new ArrayList<>();

        for (int i=0; i<attributeJoinConditions.build().length-1; i+=2) {
            orConnectedConditions.add(((ApiExpression) joinConditionArray[0]).or(joinConditionArray[1]));
        }
        ApiExpression apiExpression = orConnectedConditions.get(0);

        for (int j=1; j<orConnectedConditions.size()-1;j++) {
            apiExpression = apiExpression.and(orConnectedConditions.get(j+1));
        }
        Expression[] temporalConditions = temporalJoinConditions.build();

        for (Expression temporalCondition : temporalConditions) {
            apiExpression = apiExpression.and(temporalCondition);
        }
        return apiExpression;
    }

    /**
     * Projects needed property values from properties instance into own fields for each property.
     *
     * @return prepared vertices table
     */
    public Expression[] buildVertexGroupProjectExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(getTableEnv());

        builder.field(TableSet.FIELD_VERTEX_ID);
        builder.field(FIELD_VERTEX_EVENT_TIME);

        if (useVertexLabels) {
            builder.field(TableSet.FIELD_VERTEX_LABEL);
        }

        // grouping_property_1 AS tmp_a1, ... , grouping_property_n AS tmp_an
        for (String propertyKey : vertexGroupingPropertyKeys) {
            String propertyFieldAlias = getConfig().createUniqueAttributeName();
            builder
              .scalarFunctionCall(new ExtractPropertyValue(propertyKey), TableSet.FIELD_VERTEX_PROPERTIES)
              .as(propertyFieldAlias);
            vertexGroupingPropertyFieldNames.put(propertyKey, propertyFieldAlias);
        }

        // property_to_aggregate_1 AS tmp_b1, ... , property_to_aggregate_m AS tmp_bm
        for (CustomizedAggregationFunction aggregateFunction : vertexAggregateFunctions) {
            if (null != aggregateFunction.getPropertyKey()) {
                String propertyFieldAlias = getConfig().createUniqueAttributeName();
                builder
                  .scalarFunctionCall(
                     new ExtractPropertyValue(aggregateFunction.getPropertyKey()),
                    TableSet.FIELD_VERTEX_PROPERTIES)
                  .as(propertyFieldAlias);
                vertexAggregationPropertyFieldNames
                  .put(aggregateFunction.getAggregatePropertyKey(), propertyFieldAlias);
            }
        }
        return builder.build();
    }

    /**
     * Collects all expressions the grouped vertex table gets projected to in order to select super
     * vertices
     * <p>
     * { vertex_id, vertex_label, vertex_properties }
     *
     * @return scala sequence of expressions
     */
    protected Expression[] buildSuperVertexProjectExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(getTableEnv());

        // vertex_id
        builder.field(FIELD_SUPER_VERTEX_ID).as(TableSet.FIELD_VERTEX_ID);

        builder.field(FIELD_SUPER_VERTEX_ROWTIME).as(FIELD_EVENT_TIME);

        // vertex_label
        if (useVertexLabels) {
            builder.field(FIELD_SUPER_VERTEX_LABEL).as(TableSet.FIELD_VERTEX_LABEL);
        }

        // grouped_properties + aggregated_properties -> vertex_properties
        PlannerExpressionSeqBuilder propertyKeysAndFieldsBuilder = new PlannerExpressionSeqBuilder(getTableEnv());
        addPropertyKeyValueExpressions(
          propertyKeysAndFieldsBuilder,
          vertexGroupingPropertyKeys, vertexAfterGroupingPropertyFieldNames);

        addPropertyKeyValueExpressions(
          propertyKeysAndFieldsBuilder,
          getVertexAggregatedPropertyKeys(), vertexAfterAggregationPropertyFieldNames);

        if (propertyKeysAndFieldsBuilder.isEmpty()) {
            builder.scalarFunctionCall(new EmptyProperties());
        } else {
            builder.scalarFunctionCall(new ToProperties(),
              (new PlannerExpressionBuilder(getTableEnv())).row(propertyKeysAndFieldsBuilder.build()).getExpression());
        }
        builder.as(TableSet.FIELD_VERTEX_PROPERTIES);

        return builder.build();
    }

    /**
     * Collects all expressions the grouped edge table gets projected to in order to select super
     * edges
     * <p>
     * { edge_id, source_id, target_id, edge_label, edge_properties }
     *
     * @return scala sequence of expressions
     */
    protected Expression[] buildSuperEdgeProjectExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(getTableEnv());

        // edge_id, event_time, tail_id, head_id
        builder
          .field(FIELD_SUPER_EDGE_ID).as(TableSet.FIELD_EDGE_ID)
          .field(FIELD_EVENT_TIME)
          .field(TableSet.FIELD_SOURCE_ID)
          .field(TableSet.FIELD_TARGET_ID);

        // edge_label
        if (useEdgeLabels) {
            builder.field(TableSet.FIELD_EDGE_LABEL);
        }
        // grouped_properties + aggregated_properties -> edge_properties
        PlannerExpressionSeqBuilder propertyKeysAndFieldsBuilder = new PlannerExpressionSeqBuilder(getTableEnv());
        // add grouped properties
        addPropertyKeyValueExpressions(
          propertyKeysAndFieldsBuilder,
          edgeGroupingPropertyKeys, edgeAfterGroupingPropertyFieldNames
        );
        // add aggregated properties
        addPropertyKeyValueExpressions(
          propertyKeysAndFieldsBuilder,
          getEdgeAggregatedPropertyKeys(), edgeAfterAggregationPropertyFieldNames
        );

        if (propertyKeysAndFieldsBuilder.isEmpty()) {
            builder.scalarFunctionCall(new EmptyProperties());
        } else {
            builder.scalarFunctionCall(new ToProperties(),
              (new PlannerExpressionBuilder(getTableEnv())).row(propertyKeysAndFieldsBuilder.build()).getExpression());
        }
        builder.as(TableSet.FIELD_EDGE_PROPERTIES);

        return builder.build();
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

        WindowConfig windowConfig;

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
            this.windowConfig = WindowConfig.create();
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

        public GroupingBuilder setWindowSize(int value, WindowConfig.TimeUnit timeUnit) {
            this.windowConfig.setValue(value).setUnit(timeUnit);
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
              vertexAggregateFunctions, edgePropertyKeys, edgeAggregateFunctions, windowConfig);
        }
    }
}
