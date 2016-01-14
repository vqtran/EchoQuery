package echoquery.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import echoquery.sql.SingletonConnections;

public class VisualizationUtil {
  private static final Logger log =
      LoggerFactory.getLogger(VisualizationUtil.class);
  private static Connection conn = SingletonConnections.getStateInstance();

  public static void updateDisplayText(String message) {
    try {
      Statement statement = conn.createStatement();
      statement.executeUpdate("update sessions set display='"
          + StringEscapeUtils.escapeJava(message) + "'where id=1");
    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static String getDisplayText() {
    try {
      Statement statement = conn.createStatement();
      ResultSet result =
          statement.executeQuery("select display from sessions where id=1");
      result.first();
      return result.getString(1);
    } catch (SQLException e) {
      log.error(e.getMessage());
      return "There was an error retrieving the display text from the database";
    }
  }
}
