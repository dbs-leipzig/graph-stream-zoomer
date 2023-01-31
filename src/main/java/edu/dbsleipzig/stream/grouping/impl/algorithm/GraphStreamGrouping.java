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

import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.WindowConfig;
import edu.dbsleipzig.stream.grouping.model.graph.GraphStreamToGraphStreamOperator;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphLayout;
import edu.dbsleipzig.stream.grouping.model.table.TableSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;

import java.util.List;

import static edu.dbsleipzig.stream.grouping.model.table.TableSet.*;
import static org.apache.flink.table.api.Expressions.$;

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
      List<CustomizedAggregationFunction> edgeAggregateFunctions,
      WindowConfig windowConfig
    ) {
        super(useVertexLabels, useEdgeLabels, vertexGroupingPropertyKeys, vertexAggregateFunctions,
          edgeGroupingPropertyKeys, edgeAggregateFunctions, windowConfig);
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
        return new StreamGraph(performGrouping(), getConfig());
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
        // Returns: | vertex_event_time | vertex_id | vertex_label | vertex_properties |
        Table preparedVertices = prepareVertices();

        // 2. Write grouping or aggregating properties in own column, extract from vertex_properties column
        // returns: | vertex_id | vertex_event_time | [vertex_label] | [prop_grouping | ...] [prop_agg | ...]
        Table furtherPreparedVertices = prepareVerticesFurther(preparedVertices);

        // 3. Group vertices by label and/or property values
        // returns: | super_vertex_id | super_vertex_label | [prop_grouping | ...] [prop_agg | ...] | super_vertex_rowtime
        Table groupedVertices = groupVertices(furtherPreparedVertices);

        // 4. Derive new super vertices
        // returns: | vertex_id | vertex_label | vertex_properties |
        Table newVertices = createNewVertices(groupedVertices);

        // 5. Mapping between super-vertices and basic vertices
        // returns: | vertex_id | event_time | super_vertex_id | super_vertex_label |
        Table expandedVertices = createExpandedVertices(furtherPreparedVertices, groupedVertices);

        // 6. Assign super vertices to edges and replace source_id and target_id with the ids of the super vertices
        // returns: | edge_id | event_time | source_id | target_id | edge_label | edge_properties
        Table edgesWithSuperVertices = createEdgesWithExpandedVertices(tableSet.getEdges(), expandedVertices);

        // 7. Write grouping or aggregating properties in own column, extract from edge_properties column
        // return: | edge_id | event_time | source_id | target_id | [edge_label] | [prop_grouping | ..] [prop_agg | ... ]
        Table enrichedEdgesWithSuperVertices = enrichEdgesWithSuperVertices(edgesWithSuperVertices);

        // 8. Group edges by label and/or property values
        // return: | super_edge_id | source_id | target_id | [edge_label] | [prop_grouping | ..] [prop_agg | ... ]
        Table groupedEdges = groupEdges(enrichedEdgesWithSuperVertices);

        // 9. Derive new super edges from grouped edges
        // return: | edge_id | source_id | target_id | edge_label | edge_properties
        Table newEdges = createNewEdges(groupedEdges);

        return getConfig().getTableSetFactory().fromTables(newVertices, newEdges);
    }

    public Table prepareVertices() {
        return this.getTableEnv().sqlQuery(
          "SELECT " +
            FIELD_VERTEX_ID + " as " + FIELD_VERTEX_ID + ", " +
            FIELD_VERTEX_LABEL + " as " + FIELD_VERTEX_LABEL + ", " +
            FIELD_VERTEX_PROPERTIES + " as " + FIELD_VERTEX_PROPERTIES + ", " +
            "window_time as " + FIELD_VERTEX_EVENT_TIME + " " +
            "FROM TABLE ( TUMBLE ( TABLE  " + this.tableSet.getVertices() + ", " +
            // Todo: Replace with configurable time interval
            "DESCRIPTOR(" + FIELD_EVENT_TIME + "), " + windowConfig.getSqlApiExpression() + ")) " +
            "GROUP BY window_time, " + FIELD_VERTEX_ID + ", " + FIELD_VERTEX_LABEL + ", " +
            FIELD_VERTEX_PROPERTIES + ", window_start, window_end"
        );
    }

    public Table prepareVerticesFurther(Table preparedVertices) {
        return preparedVertices
          .select(buildVertexGroupProjectExpressions());
    }

    public Table groupVertices(Table furtherPreparedVertices){
        return furtherPreparedVertices
          // Todo: Replace with configurable time interval
          .window(Tumble.over(windowConfig.getWindowExpression()).on($(FIELD_VERTEX_EVENT_TIME)).as(FIELD_SUPER_VERTEX_EVENT_WINDOW))
          .groupBy(buildVertexGroupExpressions())
          .select(buildVertexProjectExpressions());
    }

    public Table createNewVertices(Table groupedVertices) {
        return groupedVertices
          .select(buildSuperVertexProjectExpressions());
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
     * @param furtherPreparedVertices further prepared vertex table
     * @param groupedVertices  grouped vertex table
     * @return vertex - super vertex mapping table
     */
    public Table createExpandedVertices(Table furtherPreparedVertices, Table groupedVertices) {
        return furtherPreparedVertices
          .select(buildSelectPreparedVerticesGroupAttributes())
          .join(groupedVertices.select(buildSelectGroupedVerticesGroupAttributes()))
          .where(buildJoinConditionForExpandedVertices())
          .select(buildSelectFromExpandedVertices());
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
    public Table createEdgesWithExpandedVertices(Table edges, Table expandedVertices) {
        String vertexTargetId = config.createUniqueAttributeName();
        String superVertexTargetId = config.createUniqueAttributeName();
        String vertexTargetEventTime = config.createUniqueAttributeName();
        String vertexSourceId = config.createUniqueAttributeName();
        String superVertexSourceId = config.createUniqueAttributeName();
        String vertexSourceEventTime = config.createUniqueAttributeName();

        return edges
          .join(
            expandedVertices.select(
              $(FIELD_VERTEX_ID).as(vertexTargetId),
              $(FIELD_SUPER_VERTEX_ID).as(superVertexTargetId),
              $(FIELD_EVENT_TIME).as(vertexTargetEventTime)))
          .where(
            $(FIELD_TARGET_ID).isEqual($(vertexTargetId))
              .and($(FIELD_EVENT_TIME).isLessOrEqual($(vertexTargetEventTime)))
              // Todo: Replace with configurable time interval
              .and($(FIELD_EVENT_TIME).isGreater($(vertexTargetEventTime).minus(windowConfig.getWindowExpression()))))

          .join(
            expandedVertices.select(
              $(FIELD_VERTEX_ID).as(vertexSourceId),
              $(FIELD_SUPER_VERTEX_ID).as(superVertexSourceId),
              $(FIELD_EVENT_TIME).as(vertexSourceEventTime)))
          .where(
            $(FIELD_SOURCE_ID).isEqual($(vertexSourceId))
              .and($(FIELD_EVENT_TIME).isLessOrEqual($(vertexSourceEventTime)))
              // Todo: Replace with configurable time interval
              .and($(FIELD_EVENT_TIME).isGreater($(vertexSourceEventTime).minus(windowConfig.getWindowExpression()))))

          .select(
            $(FIELD_EDGE_ID),
            $(FIELD_EVENT_TIME),
            $(superVertexSourceId).as(FIELD_SOURCE_ID),
            $(superVertexTargetId).as(FIELD_TARGET_ID),
            $(FIELD_EDGE_LABEL),
            $(FIELD_EDGE_PROPERTIES));
    }

    public Table enrichEdgesWithSuperVertices(Table edgesWithSuperVertices) {
        return edgesWithSuperVertices
          .select(buildEdgeGroupProjectExpressions());
    }

    public Table groupEdges(Table enrichedEdgesWithSuperVertices) {
        return enrichedEdgesWithSuperVertices
          // Todo: Replace with configurable time interval
          .window(Tumble.over(windowConfig.getWindowExpression()).on($(FIELD_EVENT_TIME)).as(FIELD_EDGE_EVENT_WINDOW))
          .groupBy(buildEdgeGroupExpressions())
          .select(buildEdgeProjectExpressions());
    }

    public Table createNewEdges(Table groupedEdges) {
        return groupedEdges
          .select(buildSuperEdgeProjectExpressions());
    }
}
