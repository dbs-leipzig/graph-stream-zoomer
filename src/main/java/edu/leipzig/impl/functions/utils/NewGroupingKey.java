package edu.leipzig.impl.functions.utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * The implementation of the creating the grouping key as the hash value
 * from the selecting grouping fields values
 */

public class NewGroupingKey extends ScalarFunction {

    /**
     * Returns a new grouping key
     *
     * @return new grouping key
     */

    public String eval(Object... fields) {
        StringBuilder groupingKey = new StringBuilder();
        for (Object field : fields)
            if (null != field)
                groupingKey.append(field.toString()).append(".");
        return DigestUtils.sha1Hex((groupingKey.length() > 0) ?
                groupingKey.substring(0, groupingKey.length() - 1) :
                groupingKey.toString()
        );
    }

}
