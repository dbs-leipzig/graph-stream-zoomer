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
package edu.dbsleipzig.stream.grouping.model.table;

import org.apache.flink.table.api.Table;

/**
 * Responsible for creating instances of {@link TableSetFactory}
 */
public class TableSetFactory {

    /**
     * Constructor
     */
    public TableSetFactory() { }

    /**
     * Creates a table set from given table.
     *
     * @param vertices vertices table
     * @param edges edges table
     * @return new table set
     */
    public TableSet fromTables(Table vertices, Table edges) {
        TableSet tableSet = new TableSet();

        tableSet.put(TableSet.TABLE_VERTICES, vertices);
        tableSet.put(TableSet.TABLE_EDGES, edges);

        return tableSet;
    }
}

