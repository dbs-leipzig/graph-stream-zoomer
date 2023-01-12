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
import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Iterator;

/**
 * Average user-defined aggregate function.
 */
@FunctionHint(
        output = @DataTypeHint(value= "RAW", bridgedTo = PropertyValue.class))
public class TableAvgProperty extends AggregateFunction<PropertyValue, AvgAcc> {

    @Override
    public AvgAcc createAccumulator() {
        return new AvgAcc();
    }

    @Override
    public PropertyValue getValue(AvgAcc acc) {
        if (acc.count == 0L) {
            return PropertyValue.NULL_VALUE;
        } else {
            return PropertyValue.create(acc.sum / acc.count);
        }
    }

    @FunctionHint(
            accumulator = @DataTypeHint(value = "RAW", bridgedTo = AvgAcc.class),
            input = @DataTypeHint(inputGroup = InputGroup.ANY)
    )
    public void accumulate(AvgAcc acc, Object iValueO) {
        PropertyValue iValue = (PropertyValue) iValueO;
        if (null != iValue) {
            if (iValue.isDouble()) {
                acc.sum += iValue.getDouble();
            } else if (iValue.isInt()) {
                acc.sum += iValue.getInt();
            }
            acc.count += 1L;
        }
    }

    public void retract(AvgAcc acc, PropertyValue iValue) {
        if (null != iValue) {
            if (iValue.isDouble()) {
                acc.sum -= iValue.getDouble();
            } else if (iValue.isInt()) {
                acc.sum -= iValue.getInt();
            }
            acc.count -= 1L;
        }
    }

    public void merge(AvgAcc acc, Iterable<AvgAcc> it) {
        Iterator<AvgAcc> iter = it.iterator();
        while (iter.hasNext()) {
            AvgAcc a = iter.next();
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }

    public void resetAccumulator(AvgAcc acc) {
        acc.count = 0L;
        acc.sum = 0;
    }
}
