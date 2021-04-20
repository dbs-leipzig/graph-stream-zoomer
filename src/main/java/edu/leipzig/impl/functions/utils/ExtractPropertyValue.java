package edu.leipzig.impl.functions.utils;

import org.apache.flink.table.functions.ScalarFunction;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Takes a properties object and returns property value belonging to specified key
 */
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
    
    public PropertyValue eval(Properties p) {
        return p.get(propertyKey);
    }
}
