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
package edu.dbsleipzig.stream.grouping.model.table;

import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.expressions.Expression;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper for a table based graph schema,
 * which is basically a map: tableName->{@link Schema}
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link TableSetSchema
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table;
 */
class TableSetSchema {
    /**
     * schema map
     */
    private final Map<String, Schema> schema;

    /**
     * Constructor
     *
     * @param schema immutable schema map
     */
    TableSetSchema(Map<String, Schema> schema) {
        this.schema = new HashMap<>();
        this.schema.putAll(schema);
    }

    /**
     * Returns true, iff the schema contains a table with given table name
     *
     * @param tableName table name to check
     * @return true, iff the schema contains a table with given table name
     */
    private boolean containsTable(String tableName) {
        return schema.containsKey(tableName);
    }

    /**
     * Returns the {@link Schema} for table with given table name
     *
     * @param tableName name of table to get schema for
     * @return table schema for table with given table name
     */
    private Schema getTable(String tableName) {
        if (!containsTable(tableName)) {
            throw new RuntimeException("Invalid tableName " + tableName);
        }
        return schema.get(tableName);
    }

    /**
     * Builds a sequence of expressions which can be used to project a table with a super set
     * of the fields (of the table for the given table name) to those fields only
     *
     * @param tableName name of table to get project expressions for
     * @return scala sequence of expressions
     */
    Expression[] buildProjectExpressions(String tableName) {
        return Arrays.stream((String[])getTable(tableName).getColumns().toArray()).map(Expressions::$)
          .toArray(Expression[]::new);
    }
}
