package echoquery.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import echoquery.sql.formatter.SqlFormatter;

public class Querier {

  private static final Logger log = LoggerFactory.getLogger(Querier.class);
  private Connection conn;
  private SchemaInferrer inferrer;

  public Querier(Connection conn) {
    this.conn = conn;
    this.inferrer = new SchemaInferrer(conn);
  }

  public QueryResult execute(QueryRequest request) throws SQLException {
    String sql =
        SqlFormatter.formatSql(request.buildQuery(inferrer).getQuery());
    Statement statement = conn.createStatement();
    ResultSet result = statement.executeQuery(sql);
    return new QueryResult(request, result);
  }
}
