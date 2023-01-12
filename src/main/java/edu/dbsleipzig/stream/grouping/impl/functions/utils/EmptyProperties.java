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

import org.apache.flink.table.functions.ScalarFunction;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Returns an empty new instance of {@link Properties}
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link EmptyProperties
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar;
 */

public class EmptyProperties extends ScalarFunction {

    /**
     * Returns an empty new instance of {@link Properties}
     *
     * @return new properties instance
     */
    public Properties eval() {
        return Properties.create();
    }
}
