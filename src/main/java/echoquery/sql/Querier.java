package echoquery.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.sql.tree.Query;

import echoquery.sql.formatter.SqlFormatter;

public class Querier {

  private static final Logger log = LoggerFactory.getLogger(Querier.class);

  private Connection conn;

  public Querier(Connection conn) {
    this.conn = conn;
  }

  public QueryResult execute(Query query) throws SQLException {
      String sql = SqlFormatter.formatSql(query);
      Statement statement = conn.createStatement();
      ResultSet result = statement.executeQuery(sql);
      return new QueryResult(query, result);
  }
}
