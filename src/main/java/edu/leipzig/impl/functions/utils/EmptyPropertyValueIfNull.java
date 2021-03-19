package edu.leipzig.impl.functions.utils;

import org.apache.flink.table.functions.ScalarFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Returns {@link PropertyValue#NULL_VALUE} if passed object is null
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link EmptyPropertyValueIfNull
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar;
 */
public class EmptyPropertyValueIfNull extends ScalarFunction {

    /**
     * Returns
     * - {@link PropertyValue#NULL_VALUE}, if passed object is null
     * - passed property value, otherwise
     *
     * @param pv property value or null
     * @return property value
     */
    public PropertyValue eval(PropertyValue pv) {
        if (null == pv) {
            return PropertyValue.NULL_VALUE;
        }
        return pv;
    }

    public PropertyValue eval() {
        return PropertyValue.NULL_VALUE;
    }
}
