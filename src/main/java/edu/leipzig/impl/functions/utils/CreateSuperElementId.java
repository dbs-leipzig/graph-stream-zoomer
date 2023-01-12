package edu.leipzig.impl.functions.utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.HintFlag;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * The implementation of the creating the super element id as the hash value from the selecting grouping
 * fields values.
 */
public class CreateSuperElementId extends ScalarFunction {

    /**
     * Returns a super element id based on the given fields
     *
     * @param fields the objects to consider for building the hash
     * @return new super element id
     */
    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... fields) {
        StringBuilder groupingKey = new StringBuilder();
        for (Object field : fields) {
            if (null != field) {
                groupingKey.append(field).append(".");
            }
        }

        return DigestUtils.sha1Hex((groupingKey.length() > 0) ?
                groupingKey.substring(0, groupingKey.length() - 1) :
                groupingKey.toString()
        );
    }
}
