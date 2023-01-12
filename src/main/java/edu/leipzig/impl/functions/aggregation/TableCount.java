package edu.leipzig.impl.functions.aggregation;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;

/**
 * Table Aggregate function to count elements
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 * the changes are related to using data stream instead of data set in Grable.
 *
 * @link TableCount
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate;
 */
@FunctionHint(
        output = @DataTypeHint(value= "RAW", bridgedTo = PropertyValue.class))
public class TableCount extends BaseTablePropertyValueAggregateFunction {

    @Override
    public PropertyValue createAccumulator() {
        return PropertyValue.create(0L);
    }

    @Override
    public PropertyValue getValue(PropertyValue propertyValue) {
        return propertyValue;
    }

    @FunctionHint(
            accumulator = @DataTypeHint(value = "RAW", bridgedTo = PropertyValue.class),
            input = @DataTypeHint(inputGroup = InputGroup.ANY)
    )
    public void accumulate(Object accO, Object valO) {
        PropertyValue acc = (PropertyValue) accO;
        PropertyValue val = (PropertyValue) valO;
        acc.setObject(PropertyValueUtils.Numeric.add(acc, PropertyValue.create(1L)).getObject());
    }


    public void retract(PropertyValue acc, PropertyValue val) {
        acc.setObject(PropertyValueUtils.Numeric.add(acc, PropertyValue.create(-1L)).getObject());
    }


    public void merge(PropertyValue acc, Iterable<PropertyValue> it) {
        for (PropertyValue val : it) {
            acc.setObject(PropertyValueUtils.Numeric.add(acc, val).getObject());
        }
    }


    public void resetAccumulator(PropertyValue acc) {
        acc.setLong(0L);
    }

}