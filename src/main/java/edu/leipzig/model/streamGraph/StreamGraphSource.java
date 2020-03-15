package edu.leipzig.model.streamGraph;

import edu.leipzig.impl.functions.utils.Extractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * stream graph source that takes stream of objects
 *
 * @return stream graph
 */

public class StreamGraphSource {
    DataStream<StreamObject> dataStream;

    /**
     * Creates a new stream graph source.
     *
     * @param dataStream the incoming data stream
     */
    public StreamGraphSource(DataStream<StreamObject> dataStream) {
        this.dataStream = dataStream;
    }

    /**
     * Creates a new stream graph.
     *
     * @param streamGraphConfig the stream graph configuration
     */
    public StreamGraph getStreamGraph(StreamGraphConfig streamGraphConfig) {
        SingleOutputStreamOperator<StreamEdge> edges = dataStream.process(new Extractor());
        DataStream<StreamVertex> vertices = edges.getSideOutput(Extractor.vertices);
        return new StreamGraph(vertices, edges, streamGraphConfig);
    }
}
