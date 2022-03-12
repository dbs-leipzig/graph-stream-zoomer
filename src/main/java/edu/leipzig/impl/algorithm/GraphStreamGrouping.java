package edu.leipzig.impl.algorithm;

import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.impl.functions.utils.EmptyProperties;
import edu.leipzig.impl.functions.utils.ExtractPropertyValue;
import edu.leipzig.impl.functions.utils.PlannerExpressionBuilder;
import edu.leipzig.impl.functions.utils.PlannerExpressionSeqBuilder;
import edu.leipzig.impl.functions.utils.ToProperties;
import edu.leipzig.model.graph.GraphStreamToGraphStreamOperator;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphLayout;
import edu.leipzig.model.table.TableSet;
import org.apache.flink.table.api.*;
import org.apache.flink.table.expressions.Expression;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import scala.sys.Prop;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static edu.leipzig.model.table.TableSet.*;
import static org.apache.flink.table.api.Expressions.*;

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
public class GraphStreamGrouping extends TableGroupingBase implements GraphStreamToGraphStreamOperator {

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
    GraphStreamGrouping(
      boolean useVertexLabels,
      boolean useEdgeLabels,
      List<String> vertexGroupingPropertyKeys,
      List<CustomizedAggregationFunction> vertexAggregateFunctions,
      List<String> edgeGroupingPropertyKeys,
      List<CustomizedAggregationFunction> edgeAggregateFunctions
    ) {
        super(useVertexLabels, useEdgeLabels, vertexGroupingPropertyKeys, vertexAggregateFunctions,
          edgeGroupingPropertyKeys, edgeAggregateFunctions);
    }

    /**
     * The actual computation.
     *
     * @param streamGraph layout of the stream graph
     * @return summarized, aggregated graph table set (super vertices, super edges)
     */
    @Override
    public StreamGraph execute(StreamGraphLayout streamGraph) {
        this.config = streamGraph.getConfig();
        this.tableSet = streamGraph.getTableSet();

        // Perform the grouping and create a new graph stream
        //return new StreamGraph(performGrouping(), getConfig());
        // Todo: use this first for testing issues
        return new StreamGraph(testPerformGrouping(), getConfig());
    }


    /**
     * Perform grouping based on stream graph layout and put result tables into new table set
     *
     * @return table set of result stream graph
     */
    protected TableSet performGrouping() {

        getTableEnv().createTemporaryView(TABLE_VERTICES, tableSet.getVertices());
        getTableEnv().createTemporaryView(TABLE_EDGES, tableSet.getEdges());

        // 1. Prepare distinct vertices
        Table preparedVertices = tableSet.getVertices()
          .select(buildVertexGroupProjectExpressions())
          .distinct();

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
        //  .window(Tumble.over(lit(10).seconds()).on($(FIELD_EVENT_TIME)).as("eventWindow"))
          .groupBy(buildEdgeGroupExpressions())
          .select(buildEdgeProjectExpressions());

        // 7. Derive new super edges from grouped edges
        Table newEdges = groupedEdges
          .select(buildSuperEdgeProjectExpressions());

        return getConfig().getTableSetFactory().fromTables(newVertices, newEdges);
    }

    /**
     * Perform grouping based on stream graph layout and put result tables into new table set
     *
     * @return table set of result stream graph
     */
    protected TableSet testPerformGrouping() {

        getTableEnv().createTemporaryView(TABLE_VERTICES, tableSet.getVertices());
        getTableEnv().createTemporaryView(TABLE_EDGES, tableSet.getEdges());

        // 1. Prepare distinct vertices
        // Returns: | vertex_event_time | vertex_id | vertex_label | vertex_properties |
        Table preparedVertices = getTableEnv().sqlQuery(
          "SELECT " +
                FIELD_VERTEX_ID + " as " + FIELD_VERTEX_ID + ", " +
                FIELD_VERTEX_LABEL + " as " + FIELD_VERTEX_LABEL + ", " +
                FIELD_VERTEX_PROPERTIES + " as " + FIELD_VERTEX_PROPERTIES + ", " +
                "window_time as " + FIELD_VERTEX_EVENT_TIME + " " +
            "FROM TABLE ( TUMBLE ( TABLE  " + TABLE_VERTICES + ", " +
                "DESCRIPTOR(" + FIELD_EVENT_TIME + "), INTERVAL '10' SECONDS)) " +
            "GROUP BY window_time, " + FIELD_VERTEX_ID + ", " + FIELD_VERTEX_LABEL + ", " +
                FIELD_VERTEX_PROPERTIES + ", window_start, window_end"
        );

        // 2. Write grouping or aggregating properties in own column, extract from vertex_properties column
        // returns: | vertex_id | vertex_event_time | [vertex_label] | [prop_grouping | ...] [prop_agg | ...]
        Table furtherPreparedVertices = preparedVertices
          .select(buildVertexGroupProjectExpressions());

        // 3. Group vertices by label and/or property values
        // returns: | super_vertex_id | super_vertex_label | [prop_grouping | ...] [prop_agg | ...] | super_vertex_rowtime
        Table groupedVertices = furtherPreparedVertices
          .window(Tumble.over(lit(10).seconds()).on($(FIELD_VERTEX_EVENT_TIME)).as(FIELD_SUPER_VERTEX_EVENT_WINDOW))
          .groupBy(buildVertexGroupExpressions())
          .select(buildVertexProjectExpressions());

        // 4. Derive new super vertices
        // returns: | vertex_id | vertex_label | vertex_properties |
        Table newVertices = groupedVertices
          .select(buildSuperVertexProjectExpressions());

        // 5. Mapping between super-vertices and basic vertices
        // returns: | vertex_id | event_time | super_vertex_id | super_vertex_label |
        Table expandedVertices = furtherPreparedVertices
          .select(buildSelectPreparedVerticesGroupAttributes())
          .join(groupedVertices.select(buildSelectGroupedVerticesGroupAttributes()))
          .where(buildJoinConditionForExpandedVertices())
          .select(buildSelectFromExpandedVertices());

        // 6. Assign super vertices to edges and replace source_id and target_id with the ids of the super vertices
        // returns: | edge_id | event_time | source_id | target_id | edge_label | edge_properties
        Table edgesWithSuperVertices = tableSet.getEdges()
          .join(
            expandedVertices.select(
              $(FIELD_VERTEX_ID).as("vTargetId"),
              $(FIELD_SUPER_VERTEX_ID).as("supVTargetId"),
              $(FIELD_EVENT_TIME).as("vTargetEventTime")))
          .where(
            $(FIELD_TARGET_ID).isEqual($("vTargetId"))
              .and($(FIELD_EVENT_TIME).isLessOrEqual($("vTargetEventTime")))
              .and($(FIELD_EVENT_TIME).isGreater($("vTargetEventTime").minus(lit(10).seconds()))))

          .join(
            expandedVertices.select(
              $(FIELD_VERTEX_ID).as("vSourceId"),
              $(FIELD_SUPER_VERTEX_ID).as("supVSourceId"),
              $(FIELD_EVENT_TIME).as("vSourceEventTime")))
          .where(
            $(FIELD_SOURCE_ID).isEqual($("vSourceId"))
                .and($(FIELD_EVENT_TIME).isLessOrEqual($("vSourceEventTime")))
                .and($(FIELD_EVENT_TIME).isGreater($("vSourceEventTime").minus(lit(10).seconds()))))

          .select(
            $(FIELD_EDGE_ID),
            $(FIELD_EVENT_TIME),
            $("supVSourceId").as(FIELD_SOURCE_ID),
            $("supVTargetId").as(FIELD_TARGET_ID),
            $(FIELD_EDGE_LABEL),
            $("edge_properties")
          );

        // 7. Write grouping or aggregating properties in own column, extract from edge_properties column
        // return: | edge_id | event_time | source_id | target_id | [edge_label] | [prop_grouping | ..] [prop_agg | ... ]
        Table enrichedEdgesWithSuperVertices = edgesWithSuperVertices
          .select(buildEdgeGroupProjectExpressions());

        // 8. Group edges by label and/or property values
        // return: | super_edge_id | source_id | target_id | [edge_label] | [prop_grouping | ..] [prop_agg | ... ]
        Table groupedEdges = enrichedEdgesWithSuperVertices
          .window(Tumble.over(lit(10).seconds()).on($(FIELD_EVENT_TIME)).as(FIELD_EDGE_EVENT_WINDOW))
          .groupBy(buildEdgeGroupExpressions())
          .select(buildEdgeProjectExpressions());

        // 9. Derive new super edges from grouped edges
        // return: | edge_id | source_id | target_id | edge_label | edge_properties
        Table newEdges = groupedEdges
          .select(buildSuperEdgeProjectExpressions());

        return getConfig().getTableSetFactory().fromTables(newVertices, newEdges);
    }

    /**
     * Projects needed property values from properties instance into own fields for each property.
     *
     * @return prepared vertices table
     */
    private Expression[] buildVertexGroupProjectExpressions() {
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
    private Expression[] buildSuperVertexProjectExpressions() {
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
    private Expression[] buildSuperEdgeProjectExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(getTableEnv());

        // edge_id, tail_id, head_id
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

    /**
     * Builds expression to select necessary columns from expandedVertices table
     *
     * @return scala sequence of expressions defining the selected columns
     */
    private Expression[] buildSelectFromExpandedVertices() {
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
    private Expression[] buildSelectPreparedVerticesGroupAttributes(){
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
    private Expression[] buildSelectGroupedVerticesGroupAttributes(){
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
    private Expression buildJoinConditionForExpandedVertices(){
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
          .and($("preparedVerticesTime").isGreater($(FIELD_SUPER_VERTEX_ROWTIME).minus(lit(10).seconds())));
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

        for (int k=0;k<temporalConditions.length;k++){
            apiExpression = apiExpression.and(temporalConditions[k]);
        }
        return apiExpression;
    }
}
