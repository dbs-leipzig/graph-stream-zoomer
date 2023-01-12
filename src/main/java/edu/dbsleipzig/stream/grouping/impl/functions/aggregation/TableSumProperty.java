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
package edu.dbsleipzig.stream.grouping.impl.functions.aggregation;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;

/**
 * Table Aggregate function to calculate sum of property values.
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 * the changes are related to using data stream instead of data set in Grable.
 *
 * @link TableSumProperty
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate;
 */
@FunctionHint(
        output = @DataTypeHint(value= "RAW", bridgedTo = PropertyValue.class))
public class TableSumProperty extends BaseTablePropertyValueAggregateFunction {

    @Override
    public PropertyValue createAccumulator() {
        return PropertyValue.create(0);
    }

    @Override
    public PropertyValue getValue(PropertyValue propertyValue) {
        if (propertyValue.isInt() && propertyValue.getInt() == 0) {
            return PropertyValue.NULL_VALUE;
        } else {
            return propertyValue;
        }
    }
    @FunctionHint(
            accumulator = @DataTypeHint(value = "RAW", bridgedTo = PropertyValue.class),
            input = @DataTypeHint(inputGroup = InputGroup.ANY)
    )
    public void accumulate(Object accO, Object valO) {
        PropertyValue acc = (PropertyValue) accO;
        PropertyValue val = (PropertyValue) valO;
        if (null != val) {
            acc.setObject(PropertyValueUtils.Numeric.add(acc, val).getObject());
        }
    }

    public void retract(PropertyValue acc, PropertyValue val) {
        if (null != val) {
            if (val.isInt()) {
                acc.setObject(
                  PropertyValueUtils.Numeric.add(acc, PropertyValue.create(val.getInt() * -1L)).getObject());
            } else if (val.isDouble()) {
                acc.setObject(
                  PropertyValueUtils.Numeric.add(acc, PropertyValue.create(val.getDouble() * -1L)).getObject());
            }

        }
    }

    public void merge(PropertyValue acc, Iterable<PropertyValue> it) {
        for (PropertyValue val : it) {
            acc.setObject(PropertyValueUtils.Numeric.add(acc, val).getObject());
        }
    }


    public void resetAccumulator(PropertyValue acc) {
        acc.setInt(0);
    }

}
