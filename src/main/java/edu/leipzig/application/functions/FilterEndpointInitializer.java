package edu.leipzig.application.functions;

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
