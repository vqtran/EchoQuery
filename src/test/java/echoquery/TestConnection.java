package echoquery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import echoquery.utils.EchoQueryCredentials;

public final class TestConnection {
  private static final Logger log =
      LoggerFactory.getLogger(TestConnection.class);

  private static final String URI =
      "jdbc:mysql://speechql.cq9gw5urb6jc.us-east-1.rds.amazonaws.com:3306/";
  private static final String DB = "bestbuy";

  private static Connection instance = null;
  static {
    try {
      instance = (Connection) DriverManager.getConnection(
          URI + DB, EchoQueryCredentials.dbUser, EchoQueryCredentials.dbPwd);
    } catch (SQLException e) {
      log.error(e.toString());
      System.exit(1);
    }
    log.info("EchoQuery successfully connected to " + URI + DB + ".");
  }

  public static Connection getInstance() {
    return instance;
  }
}
