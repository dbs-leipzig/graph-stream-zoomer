package edu.leipzig.impl.functions.aggregation;

import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;


import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.Sum;

/**
 * Superclass of counting aggregate functions.
 * <p>
 *  This implementation reuses much of the code of Much of Grable.
 *  the code is copied directly or has only small changes.
 *  the changes are related to using data stream instead of data set in Grable.
 *
 *  @link Count
 *  <p>
 *  references to: org.gradoop.flink.model.impl.operators.aggregation.functions.count;
 */
public class Count extends BaseAggregateFunction implements Sum, AggregateDefaultValue, CustomizedAggregationFunction {

    /**
     * Creates a new instance of a Count aggregate function.
     */
    public Count() {
        super("count");
    }

    /**
     * Creates a new instance of a Count aggregate function.
     *
     * @param aggregatePropertyKey aggregate property key
     */
    public Count(String aggregatePropertyKey) {
        super(aggregatePropertyKey);
    }

    @Override
    public PropertyValue getIncrement(Element element) {
        return PropertyValue.create(1L);
    }

    @Override
    public PropertyValue getDefaultValue() {
        return PropertyValue.create(0L);
    }

    @Override
    public AggregateFunction getTableAggFunction() {
        return new TableCount();
    }
}

