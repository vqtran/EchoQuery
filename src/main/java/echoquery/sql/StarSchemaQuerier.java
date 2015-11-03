package echoquery.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DriverManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import echoquery.utils.EchoQueryCredentials;


public class StarSchemaQuerier {
  private static final StarSchemaQuerier querier = new StarSchemaQuerier();
  private static final Logger log =
      LoggerFactory.getLogger(StarSchemaQuerier.class);
  private final Connection conn = this.getConnection();
  private final StarSchemaStore tableStore;

  private Connection getConnection() {
    String connectionUri =
        "jdbc:mysql://speechql.cutq0x5qwogl.us-east-1.rds.amazonaws.com:3306/";
    String database = "bestbuy";

    Connection conn = null;
    try {
      conn = (Connection) DriverManager.getConnection(connectionUri + database,
          EchoQueryCredentials.dbUser, EchoQueryCredentials.dbPwd);
    } catch (SQLException e) {
      log.error(e.toString());
      System.exit(1);
    }
    log.info("EchoQuery successfully connected to " + connectionUri
        + database + ".");
    return conn;
  }

  private StarSchemaQuerier() {
    this.tableStore = new StarSchemaStore(conn);
  }
  
  public static StarSchemaQuerier getInstance() {
    return querier;
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
