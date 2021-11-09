package edu.leipzig.impl.algorithm;

import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.impl.functions.utils.CreateSuperElementId;
import edu.leipzig.impl.functions.utils.EmptyProperties;
import edu.leipzig.impl.functions.utils.ExtractPropertyValue;
import edu.leipzig.impl.functions.utils.PlannerExpressionBuilder;
import edu.leipzig.impl.functions.utils.PlannerExpressionSeqBuilder;
import edu.leipzig.impl.functions.utils.ToProperties;
import edu.leipzig.model.graph.GraphStreamToGraphStreamOperator;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphLayout;
import edu.leipzig.model.table.TableSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.ScalarFunction;
import org.gradoop.common.util.GradoopConstants;

import java.util.Arrays;
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
        return new StreamGraph(testPerformGrouping(), getConfig());
        // Todo: use this first for testing issues
        // return new StreamGraph(testPerformGrouping(), getConfig());
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
     * Here we must test the windowed type of our grouping before we put it in a generalized way in our algo
     * @return
     */
    protected TableSet testPerformGrouping() {

        getTableEnv().createTemporaryView(TABLE_VERTICES, tableSet.getVertices());
        getTableEnv().createTemporaryView(TABLE_EDGES, tableSet.getEdges());


        System.out.println("Basic Vertices\n");
        tableSet.getVertices().execute().print();
        /*
        System.out.println(tableSet.getVertices().getResolvedSchema());
        tableSet.getEdges().execute().print();
        System.out.println(tableSet.getEdges().getResolvedSchema());
         */

        List<ScalarFunction> scalarFunctionsToRegister = Arrays.asList(
          new CreateSuperElementId(),
          new ToProperties()
        );

        for (ScalarFunction f : scalarFunctionsToRegister) {
            if (!Arrays.asList(getTableEnv().listUserDefinedFunctions()).contains(f.toString())) {
                // Here we use the deprecated api since the PropertyValue type is not a pojo and thus can not be
                // used in the new Flink type system
                getTableEnv().registerFunction(f.toString(), f);
            }
        }

        // 1. Prepare distinct vertices
        Table preparedVertices = tableSet.getVertices()
          .window(Tumble.over(lit(10).seconds()).on($(FIELD_EVENT_TIME)).as("eventWindow"))
          .groupBy($(FIELD_VERTEX_ID), $(FIELD_VERTEX_LABEL), $("eventWindow"))
          //.select($(FIELD_VERTEX_ID).as(FIELD_VERTEX_ID), $(FIELD_VERTEX_LABEL).as(FIELD_VERTEX_LABEL),
            //$("eventWindow").rowtime().as(FIELD_VERTEX_EVENT_TIME));
          .select(buildSuperVertexProjectExpressions());
            //$(FIELD_EVENT_TIME));

        System.out.println("Prepared Vertices\n");
        preparedVertices.execute().print();


        // group by id, label, window ; select id label window
          //.distinct();


        // 2. Group vertices by label and/or property values
        Table groupedVertices = preparedVertices
          .window(Tumble.over(lit(10).seconds()).on($(FIELD_VERTEX_EVENT_TIME)).as("eventWindow"))
          .groupBy($(FIELD_VERTEX_LABEL), $("eventWindow"))
          //.groupBy($(FIELD_VERTEX_LABEL), $(FIELD_EVENT_TIME)) // here, EVENT_TIME ist the window identifier timestamp
          .select(
            //superElementId muss auch abhängig vom event window sein?
            call("CreateSuperElementId", $(FIELD_VERTEX_LABEL)).as(FIELD_SUPER_VERTEX_ID),
            $(FIELD_VERTEX_LABEL).as(FIELD_SUPER_VERTEX_LABEL),
            lit(1).count().as("TMP12"),
            $("eventWindow").rowtime().as("vertexWindowTime")
           // $(FIELD_EVENT_TIME).as("vertexWindowTime")
            );

        System.out.println("Grouped Vertices:\n");
        groupedVertices.execute().print();

        //todo: check that there are no duplicates aggregated

        // 3. Derive new super vertices
        Table newVertices = groupedVertices
          .select(
            $(FIELD_SUPER_VERTEX_ID).as(FIELD_VERTEX_ID),
            $(FIELD_SUPER_VERTEX_LABEL).as(FIELD_VERTEX_LABEL),
            call("ToProperties", row(lit("count"), $("TMP12"))).as(FIELD_VERTEX_PROPERTIES),
            $("vertexWindowTime").as(FIELD_EVENT_TIME)
          );

        System.out.println("New Vertices\n");
        newVertices.execute().print();

        // 4. Expand a (vertex -> super vertex) mapping
        Table expandedVertices = preparedVertices
          .select($(FIELD_VERTEX_ID), $(FIELD_VERTEX_LABEL), $(FIELD_VERTEX_EVENT_TIME).as("preparedVerticesTime"))
          .join(
            groupedVertices.select($(FIELD_SUPER_VERTEX_ID), $(FIELD_SUPER_VERTEX_LABEL), $("vertexWindowTime").as("groupedVerticesTime")),
            $(FIELD_VERTEX_LABEL).isEqual($(FIELD_SUPER_VERTEX_LABEL))
              .and($("preparedVerticesTime").isLessOrEqual($("groupedVerticesTime")))
              //.and($("preparedVerticesTime").isEqual($("groupedVerticesTime")))
          )
              //.and($("preparedVerticesTime").isGreaterOrEqual($("groupedVerticesTime").minus(lit(10).seconds()))))
          .select($(FIELD_VERTEX_ID), $(FIELD_SUPER_VERTEX_ID),
            $("preparedVerticesTime").as(FIELD_EVENT_TIME));
// todo: joining preparedvertices #m with groupedVertices #n results in #m * #n pairs, maybe is solved with distinct

        System.out.println("Expanded Vertices\n");
        expandedVertices.execute().print();

        System.out.println("Edges Table:\n");
        tableSet.getEdges().execute().print();

        // 5. Assign super vertices to edges
        Table edgesWithSuperVertices = tableSet.getEdges()
          .join(
            expandedVertices.select(
              $(FIELD_VERTEX_ID).as("vTargetId"),
              $(FIELD_SUPER_VERTEX_ID).as("supVTargetId"),
              $(FIELD_EVENT_TIME).as("vTargetEventTime")),
            $(FIELD_TARGET_ID).isEqual($("vTargetId"))
            .and($(FIELD_EVENT_TIME).isLessOrEqual($("vTargetEventTime"))))
          .join(
            expandedVertices.select(
              $(FIELD_VERTEX_ID).as("vSourceId"),
              $(FIELD_SUPER_VERTEX_ID).as("supVSourceId"),
              $(FIELD_EVENT_TIME).as("vSourceEventTime")),
                $(FIELD_SOURCE_ID).isEqual($("vSourceId"))
            .and($(FIELD_EVENT_TIME).isLessOrEqual($("vSourceEventTime"))))
          .select(
            $(FIELD_EDGE_ID),
            $(FIELD_EVENT_TIME),
            $("supVSourceId").as(FIELD_SOURCE_ID),
            $("supVTargetId").as(FIELD_TARGET_ID),
            $(FIELD_EDGE_LABEL));

        System.out.println("Edges with super vertices:\n");
        edgesWithSuperVertices.execute().print();

        // 6. Group edges by label and/or property values
        Table groupedEdges = edgesWithSuperVertices
          .window(Tumble.over(lit(10).seconds()).on($(FIELD_EVENT_TIME)).as("eventWindow"))
          .groupBy($(FIELD_SOURCE_ID), $(FIELD_TARGET_ID), $(FIELD_EDGE_LABEL), $("eventWindow"))
          .select(
            call("CreateSuperElementId", $(FIELD_EDGE_LABEL)).as(FIELD_SUPER_EDGE_ID),
            $(FIELD_SOURCE_ID),
            $(FIELD_TARGET_ID),
            $(FIELD_EDGE_LABEL),
            lit(1).count().as("TMP13"),
            $("eventWindow").rowtime().as("rowtime"));

        System.out.println("Grouped edges:\n");
        groupedEdges.execute().print();

        // 7. Derive new super edges from grouped edges
        Table newEdges = groupedEdges
          .select(
            $(FIELD_SUPER_EDGE_ID).as(FIELD_EDGE_ID),
            $(FIELD_SOURCE_ID),
            $(FIELD_TARGET_ID),
            $(FIELD_EDGE_LABEL),
            call("ToProperties", row(lit("count"), $("TMP13"))).as(FIELD_EDGE_PROPERTIES),
            $("rowtime").as(FIELD_EVENT_TIME)
          );

        System.out.println("New edges:\n");
        newEdges.execute().print();

        return getConfig().getTableSetFactory().fromTables(newVertices, newEdges);
    }

    /**
     * Projects needed property values from properties instance into own fields for each property.
     *
     * @return prepared vertices table
     */
    private Expression[] buildVertexGroupProjectExpressions() {
        PlannerExpressionSeqBuilder builder = new PlannerExpressionSeqBuilder(getTableEnv());

        // vertex_id
        builder.field(TableSet.FIELD_VERTEX_ID);


        // optionally: vertex_label
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

        // vertex_label
        if (useVertexLabels) {
            builder.field(FIELD_SUPER_VERTEX_LABEL).as(TableSet.FIELD_VERTEX_LABEL);
        } else {
            builder
                    .literal(GradoopConstants.DEFAULT_VERTEX_LABEL)
                    .as(TableSet.FIELD_VERTEX_LABEL);
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
          //.field(FIELD_EVENT_TIME)
          //.field("start")
          .field(TableSet.FIELD_SOURCE_ID)
          .field(TableSet.FIELD_TARGET_ID);

        // edge_label
        if (useEdgeLabels) {
            builder.field(TableSet.FIELD_EDGE_LABEL);
        } else {
            builder
                    .literal(GradoopConstants.DEFAULT_EDGE_LABEL)
                    .as(TableSet.FIELD_EDGE_LABEL);
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
}
