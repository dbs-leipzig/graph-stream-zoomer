/*
 * Copyright © 2021 - 2024 Leipzig University (Database Research Group)
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

import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.average.Average;

import java.util.Objects;

public class AvgProperty implements Average, CustomizedAggregationFunction {

    /**
     * Property key whose value should be aggregated.
     */
    private final String propertyKey;
    /**
     * Key of the aggregate property.
     */
    private final String aggregatePropertyKey;

    /**
     * Creates a new instance of a AvgProperty aggregate function.
     *
     * @param propertyKey property key to aggregate
     */
    public AvgProperty(String propertyKey) {
        this(propertyKey, "avg_" + propertyKey);
    }

    /**
     * Creates a new instance of a AvgProperty aggregate function.
     *
     * @param propertyKey          property key to aggregate
     * @param aggregatePropertyKey aggregate property key
     */
    public AvgProperty(String propertyKey, String aggregatePropertyKey) {
        this.aggregatePropertyKey = Objects.requireNonNull(aggregatePropertyKey);
        this.propertyKey = Objects.requireNonNull(propertyKey);
    }

    @Override
    public AggregateFunction<PropertyValue, AvgAcc> getTableAggFunction() {
        return new TableAvgProperty();
    }

    @Override
    public String getAggregatePropertyKey() {
        return this.aggregatePropertyKey;
    }

    @Override
    public PropertyValue getIncrement(Element element) {
        return element.getPropertyValue(propertyKey);
    }

    @Override
    public String getPropertyKey() {
        return this.propertyKey;
    }
}
