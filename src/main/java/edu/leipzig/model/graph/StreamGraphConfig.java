package edu.leipzig.model.graph;

import edu.leipzig.model.table.TableSetFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
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
     * The table set factory.
     */
    private final TableSetFactory tableSetFactory;

    /**
     * The maximum out of orderness duration for wartermark configuration.
     */
    private Duration maxOutOfOrdernessDuration = Duration.ofSeconds(10);

    /**
     * The idle state retention time. It defines how long the state of an inactive key is at least kept
     * before it is removed.
     */
    private Duration idleStateRetentionTime = Duration.ofHours(12);

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
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        this.tableEnvironment = StreamTableEnvironment.create(env, bsSettings);
        // access flink configuration
        Configuration configuration = this.tableEnvironment.getConfig().getConfiguration();
        // set low-level key-value options
        //configuration.setString("table.exec.mini-batch.enabled", "true");  // enable mini-batch optimization
        //configuration.setString("table.exec.mini-batch.allow-latency", "5s"); // use 5 seconds to buffer input records
        // the maximum number of records can be buffered by each aggregate operator task
        //configuration.setString("table.exec.mini-batch.size", "500");
        /*
         * obtain query configuration from TableEnvironment
         * and providing a query configuration with valid retention interval to prevent excessive state size
         * */
        this.tableEnvironment.getConfig().setIdleStateRetention(this.idleStateRetentionTime);
        this.tableSetFactory = new TableSetFactory();
    }

    public Duration getMaxOutOfOrdernessDuration() {
        return maxOutOfOrdernessDuration;
    }

    public void setMaxOutOfOrdernessDuration(Duration maxOutOfOrdernessDuration) {
        this.maxOutOfOrdernessDuration = maxOutOfOrdernessDuration;
    }

    public Duration getIdleStateRetentionTime() {
        return idleStateRetentionTime;
    }

    public void setAndApplyIdleStateRetentionTime(Duration idleStateRetentionTime) {
        this.idleStateRetentionTime = idleStateRetentionTime;
        this.tableEnvironment.getConfig().setIdleStateRetention(this.idleStateRetentionTime);
    }

    /** Returns a unique temporary attribute name. */
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

}
