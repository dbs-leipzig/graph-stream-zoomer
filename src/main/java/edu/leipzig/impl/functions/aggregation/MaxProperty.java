package edu.leipzig.impl.functions.aggregation;

import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.Max;

import java.util.Objects;

/**
 * Table Aggregate function to find maximum value of a property
 * <p>
 *  This implementation reuses much of the code of Much of Grable.
 *  the code is copied directly or has only small changes.
 *
 *  @link MaxProperty
 *  <p>
 *  references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate;
 */

public class MaxProperty extends BaseAggregateFunction implements Max, CustomizedAggregationFunction {

    /**
     * Property key whose value should be aggregated.
     */
    private final String propertyKey;

    /**
     * Creates a new instance of a MaxProperty aggregate function.
     *
     * @param propertyKey property key to aggregate
     */
    public MaxProperty(String propertyKey) {
        this(propertyKey, "max_" + propertyKey);
    }

    /**
     * Creates a new instance of a MaxProperty aggregate function.
     *
     * @param propertyKey property key to aggregate
     * @param aggregatePropertyKey aggregate property key
     */
    public MaxProperty(String propertyKey, String aggregatePropertyKey) {
        super(aggregatePropertyKey);
        Objects.requireNonNull(propertyKey);
        this.propertyKey = propertyKey;
    }

    @Override
    public PropertyValue getIncrement(Element element) {
        return element.getPropertyValue(propertyKey);
    }

    @Override
    public AggregateFunction getTableAggFunction() {
        return new TableMaxProperty();
    }

    @Override
    public String getPropertyKey() {
        return this.propertyKey;
    }
}
