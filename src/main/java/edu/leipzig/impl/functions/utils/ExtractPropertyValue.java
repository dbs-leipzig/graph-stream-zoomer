package edu.leipzig.impl.functions.utils;

import org.apache.flink.table.functions.ScalarFunction;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Takes a properties object and returns property value belonging to specified key
 * besides the special case of the timestamp
 */
public class ExtractPropertyValue extends ScalarFunction {

    private final String TIMESTAMP = "timestamp";
    /**
     * Key of property to extract from properties object
     */
    private String propertyKey;
    private String unit;

    /**
     * Constructor
     *
     * @param propertyKey property key
     */
    public ExtractPropertyValue(String propertyKey) {
        if (propertyKey.startsWith(TIMESTAMP)) {
            String[] args = propertyKey.split("_");
            this.unit = (args.length > 1) ? args[1] : "";
            this.propertyKey = TIMESTAMP;
        } else
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
        if (propertyKey.equals(TIMESTAMP) && null != p.get(propertyKey)) {
            Date date = new Date(Long.parseLong(String.valueOf(p.get(propertyKey))));
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd,HH:mm:ss");
            switch (unit) {
                case "10.sec":
                    return PropertyValue.create(df.format(date).substring(0, 18).concat("0/per 10.sec"));
                case "min":
                    return PropertyValue.create(df.format(date).substring(0, 16).concat("/per min"));
                case "10.min":
                    return PropertyValue.create(df.format(date).substring(0, 15).concat("0/per 10.min"));
                case "h":
                    return PropertyValue.create(df.format(date).substring(0, 13).concat("/per hour"));
                case "d":
                    return PropertyValue.create(df.format(date).substring(0, 10).concat("/per day"));
                case "m":
                    return PropertyValue.create(df.format(date).substring(0, 7).concat("/per month"));
                case "y":
                    return PropertyValue.create(df.format(date).substring(0, 4).concat("/per year"));
                case "benchmark":
                    return PropertyValue.create(Long.parseLong(String.valueOf(p.get(propertyKey))));
                default:
                    return PropertyValue.create(df.format(date).concat("/per sec"));
            }
        } else
            return p.get(propertyKey);
    }
}
