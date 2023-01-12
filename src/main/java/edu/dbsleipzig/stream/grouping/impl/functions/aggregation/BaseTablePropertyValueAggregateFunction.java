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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Base class for all property value based table aggregation functions
 *
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has some changes.
 * the changes are related to using data stream instead of data set in Grable.
 *
 * @link BaseTablePropertyValueAggregateFunction
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate;
 */
public abstract class BaseTablePropertyValueAggregateFunction
  extends AggregateFunction<PropertyValue, PropertyValue> {

    @Override
    public abstract PropertyValue createAccumulator();

    @Override
    public abstract PropertyValue getValue(PropertyValue propertyValue);

    @Override
    public TypeInformation getResultType() {
        return TypeInformation.of(PropertyValue.class);
    }

    @Override
    public TypeInformation getAccumulatorType() {
        return TypeInformation.of(PropertyValue.class);
    }

}

