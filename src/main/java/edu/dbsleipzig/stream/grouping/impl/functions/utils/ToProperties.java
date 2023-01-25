/*
 * Copyright © 2021 - 2023 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.dbsleipzig.stream.grouping.impl.functions.utils;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Takes a row in special format and returns a properties instance
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link ToProperties
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar;
 */
@FunctionHint(output = @DataTypeHint(value= "RAW", bridgedTo = Properties.class))
public class ToProperties extends ScalarFunction {
    /**
     * Requires a 2n-ary Row
     * | key_1:String | value_1:PropertyValue | ... | key_n:String | value_n:PropertyValue|
     * and adds each key-value pair to a new Properties instance
     *
     * @param object row containing property key-value pairs
     * @return properties instance
     */
    @FunctionHint(input = @DataTypeHint(inputGroup = InputGroup.ANY))
    @SuppressWarnings("unused")
    public Properties eval(Object object) {
        Row row = (Row) object;

        Properties properties = Properties.create();

        if (row.getArity() % 2 != 0) {
            throw new RuntimeException("Arity of given row is odd and therefore can't get processed");
        }

        for (int i = 0; i < row.getArity(); i = i + 2) {
            if (null == row.getField(i + 1)) {
                continue;
            }
            Object f0 = row.getField(i);
            if (!(f0 instanceof String)) {
                throw new RuntimeException("Odd expression of property row must be a property key string");
            }
            Object f1 = row.getField(i + 1);
            if (f1 instanceof Long) {
                f1 = PropertyValue.create(f1);
            } else if (!(f1 instanceof PropertyValue)) {
                throw new RuntimeException("Even expression of property row must be of type " +
                  "[Long, PropertyValue].");
            }

            String key = (String) f0;
            // todo: check if this trimming is cool, because literals got '...' quotes
            if (key.startsWith("'") && key.endsWith("'")) {
                key = key.substring(1, key.length() - 1);
            }
            PropertyValue value = (PropertyValue) f1;

            properties.set(key, value);
        }
        return properties;
    }
}
