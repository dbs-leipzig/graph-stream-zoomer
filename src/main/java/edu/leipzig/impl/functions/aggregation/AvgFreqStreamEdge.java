package edu.leipzig.impl.functions.aggregation;

import org.apache.flink.table.functions.AggregateFunction;


public class AvgFreqStreamEdge implements CustomizedAggregationFunction {

    /**
     * Key of the aggregate property.
     */
    private String aggregatePropertyKey;

    /**
     * Creates a new instance of a AvgFreqStreamEdge aggregate function.
     */
    public AvgFreqStreamEdge() {
        this.aggregatePropertyKey = "avgFreq/s";
    }

    @Override
    public AggregateFunction getTableAggFunction() {
        return new TableAvgFreqStreamEdge();
    }

    @Override
    public String getAggregatePropertyKey() {
        return this.aggregatePropertyKey;
    }
}
