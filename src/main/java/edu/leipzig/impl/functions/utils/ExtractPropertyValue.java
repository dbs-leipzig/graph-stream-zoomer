package edu.leipzig.impl.functions.utils;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Takes a properties object and returns property value belonging to specified key
 */
@FunctionHint(
        input = @DataTypeHint(bridgedTo = Properties.class, allowRawPattern = "TRUE"),
        output = @DataTypeHint(bridgedTo = PropertyValue.class))
public class ExtractPropertyValue extends ScalarFunction {

    /**
     * Key of property to extract from properties object
     */
    private final String propertyKey;

    /**
     * Constructor
     *
     * @param propertyKey property key
     */
    public ExtractPropertyValue(String propertyKey) {
        this.propertyKey = propertyKey;
    }

    /**
     * Returns property value of given properties object belonging to property key defined in
     * constructor call
     *
     * @param p properties object
     * @return property value belonging to specified key
     */
    @DataTypeHint(bridgedTo = PropertyValue.class)
    public PropertyValue eval(@DataTypeHint(bridgedTo = Properties.class,
            allowRawPattern = "org.gradoop.common.model.impl.properties") Properties p) {
        return p.get(propertyKey);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return super.getTypeInference(typeFactory);
    }
}
