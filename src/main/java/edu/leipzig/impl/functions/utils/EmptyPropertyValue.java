package edu.leipzig.impl.functions.utils;

import org.apache.flink.table.functions.ScalarFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Returns property value null value
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link EmptyPropertyValue
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar;
 */
/*@FunctionHint(
  output = @DataTypeHint(value = "LEGACY", bridgedTo = PropertyValue.class, allowRawGlobally = TRUE)
)*/
public class EmptyPropertyValue extends ScalarFunction {
    /**
     * Returns {@link PropertyValue#NULL_VALUE}
     * @return property value null value
     */
    public PropertyValue eval() {
        return PropertyValue.NULL_VALUE;
    }
}