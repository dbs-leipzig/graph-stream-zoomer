package edu.leipzig.impl.algorithm;

import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Base class for all builders of table based grouping operators
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link TableGroupingBuilderBase
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.operators.grouping;
 */
public abstract class TableGroupingBuilderBase {

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
    TableGroupingBuilderBase() {
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
    private TableGroupingBuilderBase addVertexGroupingKey(String key) {
        Objects.requireNonNull(key);
        if (key.equals(LABEL_SYMBOL)) {
            useVertexLabel(true);
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
    public TableGroupingBuilderBase addVertexGroupingKeys(List<String> keys) {
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
    private TableGroupingBuilderBase addEdgeGroupingKey(String key) {
        Objects.requireNonNull(key);
        if (key.equals(LABEL_SYMBOL)) {
            useEdgeLabel(true);
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
    public TableGroupingBuilderBase addEdgeGroupingKeys(List<String> keys) {
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
    public TableGroupingBuilderBase addVertexAggregateFunction(CustomizedAggregationFunction aggregateFunction) {
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
    public TableGroupingBuilderBase addEdgeAggregateFunction(CustomizedAggregationFunction aggregateFunction) {
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
    private TableGroupingBuilderBase useVertexLabel(boolean useVertexLabel) {
        this.useVertexLabel = useVertexLabel;
        return this;
    }

    /**
     * Define, if the edge label shall be used for grouping edges.
     *
     * @param useEdgeLabel true, iff edge label shall be used for grouping
     * @return this builder
     */
    private TableGroupingBuilderBase useEdgeLabel(boolean useEdgeLabel) {
        this.useEdgeLabel = useEdgeLabel;
        return this;
    }

    /**
     * Build grouping operator instance
     *
     * @return grouping operator instance
     */
    public abstract TableGroupingBase build();

}