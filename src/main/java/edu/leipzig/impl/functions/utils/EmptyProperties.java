package edu.leipzig.impl.functions.utils;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Returns an empty new instance of {@link Properties}
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link EmptyProperties
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar;
 */
@FunctionHint(
    output = @DataTypeHint(value = "RAW", bridgedTo = Properties.class)
)
public class EmptyProperties extends ScalarFunction {

    /**
     * Returns an empty new instance of {@link Properties}
     *
     * @return new properties instance
     */
    public Properties eval() {
        return Properties.create();
    }
}
