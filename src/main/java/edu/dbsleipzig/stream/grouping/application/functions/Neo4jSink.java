package edu.dbsleipzig.stream.grouping.application.functions;

import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.util.HashMap;
import java.util.Map;


public class Neo4jSink extends RichSinkFunction<StreamTriple> {

  Session session;

  // URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
  final String dbUri = "neo4j://localhost";
  final String dbUser = "neo4j";
  final String dbPassword = "password";

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    try (Driver driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword))) {
      driver.verifyConnectivity();
      System.out.println("Connection established.");
      this.session = driver.session();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
  }

  @Override
  public void invoke(StreamTriple streamTriple, Context context) throws Exception {
    super.invoke(streamTriple, context);

    try {
      writeTriple(streamTriple);
      System.out.println("Triple added to database.");
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }

  }

  private void writeTriple(StreamTriple streamTriple) {
    String query = "MATCH (s1:Station), (s2:Station) WHERE elementId(s1) = $srcId AND elementId(s2) = $trgId " +
      "WITH s1, s2 " +
      "CREATE (s1)-[t:CDC_TRIP]->(s2) " +
      "SET t.operation = $operation, " +
      "t.count = $count ";

    Map<String, Object> params = new HashMap<>();
    params.put("srcId", streamTriple.getSource().getVertexId());
    params.put("trgId", streamTriple.getTarget().getVertexId());
    params.put("count", streamTriple.getProperties().get("count"));


    this.session.run(query, params);
  }
}
