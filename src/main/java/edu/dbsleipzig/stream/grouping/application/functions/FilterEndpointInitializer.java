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
package edu.dbsleipzig.stream.grouping.application.functions;

import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.Collections;

/**
 * Twitter endpoint initializer with a bounding box of Germany.
 */
public class FilterEndpointInitializer implements TwitterSource.EndpointInitializer, Serializable {
  @Override
  public StreamingEndpoint createEndpoint() {
    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    endpoint.locations(Collections.singletonList(
      new Location(
        // Germany bounding box
        new Location.Coordinate(5.779, 47.205),
        new Location.Coordinate(15.161, 55.076))));
    endpoint.stallWarnings(false);
    endpoint.delimited(false);
    return endpoint;
  }
}
