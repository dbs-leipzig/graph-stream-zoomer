package edu.leipzig.impl.functions.utils;

import edu.leipzig.model.streamGraph.StreamEdge;
import edu.leipzig.model.streamGraph.StreamObject;
import edu.leipzig.model.streamGraph.StreamVertex;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The implementation of the ProcessFunction that extracts the edges from data stream objects as regular output
 * besides their vertices as side output
 */

public class Extractor extends ProcessFunction<StreamObject, StreamEdge> {
    public static final OutputTag<StreamVertex> vertices = new OutputTag<StreamVertex>("side-output") {
    };

    @Override
    public void processElement(StreamObject value, Context ctx, Collector<StreamEdge> out) throws Exception {
        // emit data to side output
        ctx.output(vertices, value.getSource());
        ctx.output(vertices, value.getTarget());
        // emit data to regular output
        out.collect(value.getEdge());
    }
}
