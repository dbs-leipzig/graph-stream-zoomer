package edu.leipzig.impl.functions.aggregation;

import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.Min;

import java.util.Objects;

/**
 * Table Aggregate function to find minimum value of a property
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link MinProperty
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate;
 */

public class MinProperty extends BaseAggregateFunction implements Min, CustomizedAggregationFunction {

    /**
     * Property key whose value should be aggregated.
     */
    private final String propertyKey;

    /**
     * Creates a new instance of a MinProperty aggregate function.
     *
     * @param propertyKey property key to aggregate
     */
    public MinProperty(String propertyKey) {
        this(propertyKey, "min_" + propertyKey);
    }

    /**
     * Creates a new instance of a MinProperty aggregate function.
     *
     * @param propertyKey          property key to aggregate
     * @param aggregatePropertyKey aggregate property key
     */
    public MinProperty(String propertyKey, String aggregatePropertyKey) {
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
        return new TableMinProperty();
    }

    @Override
    public String getPropertyKey() {
        return this.propertyKey;
    }
}