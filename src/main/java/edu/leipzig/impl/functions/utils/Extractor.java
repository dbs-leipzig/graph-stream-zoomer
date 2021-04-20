package edu.leipzig.impl.functions.utils;

import edu.leipzig.model.graph.StreamEdge;
import edu.leipzig.model.graph.StreamTriple;
import edu.leipzig.model.graph.StreamVertex;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The implementation of the ProcessFunction that extracts the edges from data stream objects as regular output
 * besides their vertices as side output
 */

public class Extractor extends ProcessFunction<StreamTriple, StreamEdge> {

    public static final OutputTag<StreamVertex> VERTEX_OUTPUT_TAG = new OutputTag<StreamVertex>("side-output") {};

    @Override
    public void processElement(StreamTriple value, Context ctx, Collector<StreamEdge> out) {
        // emit data to side output
        ctx.output(VERTEX_OUTPUT_TAG, value.getSource());
        ctx.output(VERTEX_OUTPUT_TAG, value.getTarget());
        // emit data to regular output
        out.collect(value.getEdge());
    }
}
