package echoquery.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.speechlet.Session;

import echoquery.sql.SingletonConnections;

public class VisualizationUtil {
  private static final Logger log =
      LoggerFactory.getLogger(VisualizationUtil.class);
  private static Connection conn = SingletonConnections.getStateInstance();

  public static void updateDisplayText(String message, Session session) {
    try {
      makeSureSessionExistsInDB(session.getUser().getUserId());
      Statement statement = conn.createStatement();
      statement.executeUpdate("update sessions set display='" +
          StringEscapeUtils.escapeJava(message) + "'where id='" +
          cleanId(session.getUser().getUserId()) + "';");
    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static String getDisplayText(String userId) {
    try {
      makeSureSessionExistsInDB(userId);
      Statement statement = conn.createStatement();
      ResultSet result = statement.executeQuery(
          "select display from sessions where id='" + cleanId(userId) + "';");
      result.first();
      return result.getString(1);
    } catch (SQLException e) {
      log.error(e.getMessage());
      return "There was an error retrieving the display text from the database";
    }
  }

  // Since id is a unique column in the sessions table, we don't need to worry
  // about duplicate rows being inserted
  public static void makeSureSessionExistsInDB(String userId) {
      try {
        Statement statement = conn.createStatement();
        statement.executeUpdate("insert into sessions values ('" +
            cleanId(userId) + "', 'EchoQuery. What do you want?');");
      } catch (SQLException e) {
        // silently ignore - we already have this value in the db
      }
  }

  // removes the text at the beginning of the id
  public static String cleanId(String userId) {
    String[] splitted = userId.split("\\.");
    if (splitted.length == 3) {
      return splitted[2];
    } else {
      return splitted[0];
    }
  }
}