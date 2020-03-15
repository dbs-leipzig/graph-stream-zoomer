package edu.leipzig.model.streamGraph;

import edu.leipzig.model.table.TableSetFactory;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * stream graph configuration.
 */

public class StreamGraphConfig {
    /**
     * Flink stream execution environment.
     */
    private final StreamExecutionEnvironment executionEnvironment;

    /**
     * Flink stream table execution environment.
     */
    private final StreamTableEnvironment tableEnvironment;

    /**
     * Flink table stream query configuration.
     */
    // private final StreamQueryConfig qConfig;

    /**
     * table set factory.
     */
    private final TableSetFactory tableSetFactory;

    // a counter for unique attribute names
    AtomicInteger attrNameCtr = new AtomicInteger(0);

    /**
     * Creates a new stream graph Configuration.
     *
     * @param env              Flink stream execution environment
     * @param minRetentionTime The minimum idle state retention time defines how long
     *                         the state of an inactive key is at least kept before it is removed.
     * @param maxRetentionTime The maximum idle state retention time defines how long
     *                         the state of an inactive key is at most kept before it is removed.
     */
    public StreamGraphConfig(StreamExecutionEnvironment env, long minRetentionTime, long maxRetentionTime) {
        this.executionEnvironment = env;

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        this.tableEnvironment = StreamTableEnvironment.create(env, bsSettings);
        // access flink configuration
        Configuration configuration = this.tableEnvironment.getConfig().getConfiguration();
        // set low-level key-value options
        configuration.setString("table.exec.mini-batch.enabled", "true");
        configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
        configuration.setString("table.exec.mini-batch.size", "5000");
        /*
         * obtain query configuration from TableEnvironment
         * and providing a query configuration with valid retention interval to prevent excessive state size
         * */
        this.tableEnvironment.getConfig().setIdleStateRetentionTime(Time.hours(minRetentionTime), Time.hours(maxRetentionTime));
        // this.qConfig.withIdleStateRetentionTime(Time.hours(minRetentionTime), Time.hours(maxRetentionTime));
        this.tableSetFactory = new TableSetFactory();
    }


    /** Returns a unique temporary attribute name. */
    public String createUniqueAttributeName() {
        return "TMP_" + attrNameCtr.getAndIncrement();
    }

    /**
     * Returns the Flink table stream query configuration.
     *
     * @return Flink table stream query configuration.
     */
    /*public StreamQueryConfig getQConfig() {
        return qConfig;
    }*/

    /**
     * Returns the table set factory.
     *
     * @return table set factory.
     */
    public TableSetFactory getTableSetFactory() {
        return tableSetFactory;
    }

    /**
     * Returns the Flink stream execution environment.
     *
     * @return Flink stream execution environment
     */
    public StreamExecutionEnvironment getExecutionEnvironment() {
        return executionEnvironment;
    }

    /**
     * Returns the Flink stream table environment.
     *
     * @return Flink stream table environment
     */
    public StreamTableEnvironment getTableEnvironment() {
        return tableEnvironment;
    }

}
