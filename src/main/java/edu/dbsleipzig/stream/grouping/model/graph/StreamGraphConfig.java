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
package edu.dbsleipzig.stream.grouping.model.graph;

import edu.dbsleipzig.stream.grouping.model.table.TableSetFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The stream graph configuration.
 */
public class StreamGraphConfig {

    /**
     * Flink stream table execution environment.
     */
    private final StreamTableEnvironment tableEnvironment;

    /**
     * Flink stream execution environment.
     */
    private final StreamExecutionEnvironment streamEnvironment;

    /**
     * The table set factory.
     */
    private final TableSetFactory tableSetFactory;

    /**
     * The maximum out of orderness duration for wartermark configuration.
     */
    private Duration maxOutOfOrdernessDuration = Duration.ofSeconds(10);

    /**
     * A counter for unique attribute names.
     */
    AtomicInteger attrNameCtr = new AtomicInteger(0);

    /**
     * Creates a new stream graph Configuration.
     *
     * @param env Flink stream execution environment
     */
    public StreamGraphConfig(StreamExecutionEnvironment env) {
        this.streamEnvironment = Objects.requireNonNull(env);
        EnvironmentSettings bsSettings = EnvironmentSettings
          .newInstance()
          .inStreamingMode()
          .build();
        this.tableEnvironment = StreamTableEnvironment.create(env, bsSettings);
        //this.tableEnvironment = StreamTableEnvironment.create(env);
        // access flink configuration
        //Configuration configuration = this.tableEnvironment.getConfig().getConfiguration();
        // set low-level key-value options
        //configuration.setString("table.exec.mini-batch.enabled", "true");  // enable mini-batch optimization
        //configuration.setString("table.exec.mini-batch.allow-latency", "5s"); // use 5 seconds to buffer input records
        // the maximum number of records can be buffered by each aggregate operator task
        //configuration.setString("table.exec.mini-batch.size", "500");
        /*
         * obtain query configuration from TableEnvironment
         * and providing a query configuration with valid retention interval to prevent excessive state size
         * */
        //this.tableEnvironment.getConfig().setIdleStateRetention(this.idleStateRetentionTime);
        this.tableSetFactory = new TableSetFactory();
    }

    /**
     * Get the configured out-of-orderness duration.
     *
     * @return the duration used as out-of-orderness configuration
     */
    public Duration getMaxOutOfOrdernessDuration() {
        return maxOutOfOrdernessDuration;
    }

    public void setMaxOutOfOrdernessDuration(Duration maxOutOfOrdernessDuration) {
        this.maxOutOfOrdernessDuration = maxOutOfOrdernessDuration;
    }

    /**
     * Returns a unique temporary attribute name.
     *
     * @return a unique temporary attribute name as String.
     */
    public String createUniqueAttributeName() {
        return "TMP_" + attrNameCtr.getAndIncrement();
    }

    /**
     * Returns the table set factory.
     *
     * @return table set factory.
     */
    public TableSetFactory getTableSetFactory() {
        return tableSetFactory;
    }

    /**
     * Returns the Flink stream table environment.
     *
     * @return Flink stream table environment
     */
    public StreamTableEnvironment getTableEnvironment() {
        return tableEnvironment;
    }

    /**
     * Returns the Flink stream environment.
     *
     * @return Flink stream environment
     */
    public StreamExecutionEnvironment getStreamEnvironment() {
        return streamEnvironment;
    }
}
