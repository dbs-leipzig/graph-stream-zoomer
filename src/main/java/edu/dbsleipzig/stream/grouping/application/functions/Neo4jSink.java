package edu.dbsleipzig.stream.grouping.application.functions;

import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;


public class Neo4jSink extends RichSinkFunction<StreamTriple> {

  Driver driver;

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
      this.driver = driver;
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
  }

  @Override
  public void invoke(StreamTriple streamTriple, Context context) throws Exception {
    super.invoke(streamTriple, context);

    try (Session session = driver.session(SessionConfig.builder().withDatabase("neo4j").build())) {

        try {
          String id = session.writeTransaction(tx -> writeTriple(tx, streamTriple));
          System.out.printf("Triple %s added to database.", id);
        } catch (Exception e) {
          System.out.println(e.getMessage());
        }
    }
  }

  private String writeTriple(Transaction tx, StreamTriple streamTriple) {
    String query = "MATCH (s1:Station), (s2:Station) WHERE elementId(s1) = $srcId AND elementId(s2) = $trgId " +
      "WITH s1, s2 " +
      "CREATE (s1)-[t:CDC_TRIP]->(s2) " +
      "SET t.operation = $operation " +
      "SET t.count = $count ";

    return null;
  }
}
