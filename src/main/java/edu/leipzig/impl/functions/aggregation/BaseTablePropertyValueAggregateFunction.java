package edu.leipzig.impl.functions.aggregation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

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

    @Override
    public TypeInformation getResultType() {
        return TypeInformation.of(PropertyValue.class);
    }

    @Override
    public TypeInformation getAccumulatorType() {
        return TypeInformation.of(PropertyValue.class);
    }

}

