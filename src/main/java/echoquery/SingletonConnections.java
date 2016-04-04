package echoquery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import echoquery.utils.EchoQueryCredentials;

public final class SingletonConnections {
  private static final Logger log =
      LoggerFactory.getLogger(SingletonConnections.class);

  private static final String URI =
      "jdbc:mysql://speechql.cq9gw5urb6jc.us-east-1.rds.amazonaws.com:3306/";
  private static final String dataDB = "mimic";
  private static final String stateDB = "sessions";

  private static Connection dataInstance = null;
  private static Connection stateInstance = null;
  static {
    try {
      dataInstance = (Connection) DriverManager.getConnection(
          URI + dataDB,
          EchoQueryCredentials.dbUser,
          EchoQueryCredentials.dbPwd);
      stateInstance = (Connection) DriverManager.getConnection(
          URI + stateDB,
          EchoQueryCredentials.dbUser,
          EchoQueryCredentials.dbPwd);
    } catch (SQLException e) {
      log.error(e.toString());
      System.exit(1);
    }
    log.info("EchoQuery successfully connected to "
        + URI + dataDB + " and " + stateDB + ".");
  }

  public static Connection getDataInstance() {
    return dataInstance;
  }

  public static Connection getStateInstance() {
    return stateInstance;
  }
}
