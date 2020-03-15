package edu.leipzig.impl.functions.aggregation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Base class for all property value based table aggregation functions
 *
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has some changes.
 * the changes are related to using data stream instead of data set in Grable.
 *
 * @link BaseTablePropertyValueAggregateFunction
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate;
 */
public abstract class BaseTablePropertyValueAggregateFunction
        extends AggregateFunction<PropertyValue, PropertyValue> {

    @Override
    public abstract PropertyValue createAccumulator();

    @Override
    public abstract PropertyValue getValue(PropertyValue propertyValue);

    /**
     * Processes the input property values and update the provided property value instance.
     *
     * @param accumulator the property value which contains the current aggregated results
     * @param value       the input property value
     */
    public abstract void accumulate(PropertyValue accumulator, PropertyValue value);
    /**
     * Retracts the input values from the accumulator instance. The current design assumes the
     * inputs are the values that have been previously accumulated. The method retract can be
     * overloaded with different custom types and arguments. This function must be implemented for
     * datastream bounded over aggregate.
     *
     * @param accumulator  the accumulator which contains the current aggregated results
     * @param value       the input property value
     */
    public abstract void retract(PropertyValue accumulator, PropertyValue value);

    /**
     * Merges a group of property value instances into one property value.
     *
     * @param accumulator the property value which will keep the merged aggregate results.
     * @param its         an [[java.lang.Iterable]] pointed to a group of property value instances
     *                    that will be merged.
     */
    public abstract void merge(PropertyValue accumulator, Iterable<PropertyValue> its);

    /**
     * Resets the property value.
     *
     * @param accumulator the property value which needs to be reset
     */
    public abstract void resetAccumulator(PropertyValue accumulator);

    @Override
    public TypeInformation getResultType() {
        return TypeInformation.of(PropertyValue.class);
    }

    @Override
    public TypeInformation getAccumulatorType() {
        return TypeInformation.of(PropertyValue.class);
    }

}

