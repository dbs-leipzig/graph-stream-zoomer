package edu.leipzig.impl.functions.aggregation;

import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;

/**
 * Describes an aggregate function as input for the {@link Aggregation} operator.
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link AggregateFunction
 * <p>
 * references to: org.gradoop.flink.model.api.functions;
 */

public interface CustomizedAggregationFunction {

    /**
     * Returns property key whose value should be aggregated
     *
     * @return property key
     */
    default String getPropertyKey() {
        return null;
    }

    /**
     * Registered name of aggregation function for use within Table-API
     *
     * @return name of aggregation function
     */
    default org.apache.flink.table.functions.AggregateFunction getTableAggFunction() {
        throw new RuntimeException("AggregateFunction " + getClass().getName() + " is not prepared " +
                "for use within Flink's Table API");
    }

    /**
     * Returns the property key used to store the aggregate value.
     *
     * @return aggregate property key
     */
    String getAggregatePropertyKey();
}
