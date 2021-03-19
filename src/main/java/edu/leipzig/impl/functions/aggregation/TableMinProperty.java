package edu.leipzig.impl.functions.aggregation;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;

/**
 * Table Aggregate function to find minimum value of a property
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 * the changes are related to using data stream instead of data set in Grable.
 *
 * @link TableMinProperty
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate;
 */

public class TableMinProperty  extends BaseTablePropertyValueAggregateFunction {

    @Override
    public PropertyValue createAccumulator() {
        return PropertyValue.create(Double.MAX_VALUE);
    }

    @Override
    public PropertyValue getValue(PropertyValue propertyValue) {
        if (propertyValue.isDouble() &&
                Double.compare(propertyValue.getDouble(), Double.MAX_VALUE) == 0) {
            return PropertyValue.NULL_VALUE;
        } else {
            return propertyValue;
        }
    }


    public void accumulate(PropertyValue acc, PropertyValue val) {
        if (null != val) {
            acc.setObject(PropertyValueUtils.Numeric.min(acc, val).getObject());
        }
    }


    public void retract (PropertyValue acc, PropertyValue val) {
    }


    public void merge(PropertyValue acc, Iterable<PropertyValue> it) {
        for (PropertyValue val : it) {
            acc.setObject(PropertyValueUtils.Numeric.min(acc, val).getObject());
        }
    }


    public void resetAccumulator(PropertyValue acc) {
        acc.setDouble(Double.MAX_VALUE);
    }
}
