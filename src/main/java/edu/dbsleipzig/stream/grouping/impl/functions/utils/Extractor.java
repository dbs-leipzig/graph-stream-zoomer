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
package edu.dbsleipzig.stream.grouping.impl.functions.utils;

import edu.dbsleipzig.stream.grouping.model.graph.StreamEdge;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The implementation of the ProcessFunction that extracts the edges from data stream objects as regular
 * output besides their vertices as side output.
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
