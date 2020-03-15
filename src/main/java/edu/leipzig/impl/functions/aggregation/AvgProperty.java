package edu.leipzig.impl.functions.aggregation;

import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.average.Average;

import java.util.Objects;

public class AvgProperty implements Average, CustomizedAggregationFunction {

    /**
     * Property key whose value should be aggregated.
     */
    private final String propertyKey;
    /**
     * Key of the aggregate property.
     */
    private String aggregatePropertyKey;

    /**
     * Creates a new instance of a AvgProperty aggregate function.
     *
     * @param propertyKey property key to aggregate
     */
    public AvgProperty(String propertyKey) {
        this(propertyKey, "avg_" + propertyKey);
    }

    /**
     * Creates a new instance of a AvgProperty aggregate function.
     *
     * @param propertyKey          property key to aggregate
     * @param aggregatePropertyKey aggregate property key
     */
    public AvgProperty(String propertyKey, String aggregatePropertyKey) {
        Objects.requireNonNull(aggregatePropertyKey);
        this.aggregatePropertyKey = aggregatePropertyKey;
        Objects.requireNonNull(propertyKey);
        this.propertyKey = propertyKey;
    }

    @Override
    public AggregateFunction getTableAggFunction() {
        return new TableAvgProperty();
    }

    @Override
    public String getAggregatePropertyKey() {
        return this.aggregatePropertyKey;
    }

    @Override
    public PropertyValue getIncrement(Element element) {
        return element.getPropertyValue(propertyKey);
    }

    @Override
    public String getPropertyKey() {
        return this.propertyKey;
    }
}
