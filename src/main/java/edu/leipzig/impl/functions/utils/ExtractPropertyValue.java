package edu.leipzig.impl.functions.utils;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.HintFlag;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.*;
import org.apache.flink.table.types.inference.strategies.AnyArgumentTypeStrategy;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.List;
import java.util.Optional;

/**
 * Takes a properties object and returns property value belonging to specified key
 */
@FunctionHint(
        output = @DataTypeHint(value= "RAW", bridgedTo = PropertyValue.class))
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
    @FunctionHint(
            input = @DataTypeHint(inputGroup = InputGroup.ANY))
    public PropertyValue eval( Object o) {
        Properties p = (Properties) o;
        return p.get(propertyKey);
    }
}
