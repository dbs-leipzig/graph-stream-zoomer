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
import org.apache.flink.table.functions.ScalarFunction;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Takes a properties object and returns property value belonging to specified key
 */
@FunctionHint(output = @DataTypeHint(value= "RAW", bridgedTo = PropertyValue.class))
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
     * constructor call.
     *
     * @param properties properties object
     * @return property value belonging to specified key
     */
    @FunctionHint(input = @DataTypeHint(value= "RAW", bridgedTo = Properties.class))
    @SuppressWarnings("unused")
    public PropertyValue eval(Properties properties) {
        return properties.get(propertyKey);
    }
}
