package echoquery.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DriverManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import echoquery.sql.model.ColumnValue;
import echoquery.utils.EchoQueryCredentials;

public class Querier {

  private static final Logger log =
      LoggerFactory.getLogger(Querier.class);

  private Connection conn;
  private SchemaInferrer inferrer;

  private Querier() {
    this.conn = SingletonConnection.getInstance();
    this.inferrer = new SchemaInferrer(conn);
  }

  public int count(String table) throws SQLException {
      Statement statement = conn.createStatement();
      ResultSet result =
          statement.executeQuery("select count(*) from " + table);
      result.first();
      return result.getInt(1);
  }

  public int countWhere(String table, String column, ColumnValue value) {
    return -1;
  }
}
