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

import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;

/**
 * Describes an aggregate function as input for the {@link Aggregation} operator.
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link AggregateFunction
 * <p>
 * references to: org.gradoop.flink.model.api.functions;
 */

public interface CustomizedAggregationFunction {

    /**
     * Returns property key whose value should be aggregated
     *
     * @return property key
     */

    default String getPropertyKey() {
        return null;
    }

    /**
     * Registered name of aggregation function for use within Table-API
     *
     * @return name of aggregation function
     */
    default AggregateFunction<PropertyValue, ?> getTableAggFunction() {
        throw new RuntimeException("AggregateFunction " + getClass().getName() +
          " is not prepared for use within Flink's Table API.");
    }

    /**
     * Returns the property key used to store the aggregate value.
     *
     * @return aggregate property key
     */
    String getAggregatePropertyKey();
}
