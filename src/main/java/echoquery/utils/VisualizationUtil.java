package echoquery.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.speechlet.Session;
import com.google.common.base.Joiner;

import echoquery.SingletonConnections;
import echoquery.querier.ResultTable;

public class VisualizationUtil {
  private static final Logger log =
      LoggerFactory.getLogger(VisualizationUtil.class);
  private static Connection conn = SingletonConnections.getStateInstance();

  public static void updateDisplayText(String message, Session session) {
    try {
      makeSureSessionExistsInDB(session.getUser().getUserId());
      Statement statement = conn.createStatement();
      statement.executeUpdate("update sessions set display='" +
          StringEscapeUtils.escapeJava(message) + "' where id='" +
          cleanId(session.getUser().getUserId()) + "';");
    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static void updateResultData(JSONObject data, Session session) {
    try {
      makeSureSessionExistsInDB(session.getUser().getUserId());
      Statement statement = conn.createStatement();
      statement.executeUpdate("update sessions set result='" +
          StringEscapeUtils.escapeJava(data.toString()) + "'where id='" +
          cleanId(session.getUser().getUserId()) + "';");
    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static void updatePlotColumns(List<String> plotCols, Session session) {
    try {
      makeSureSessionExistsInDB(session.getUser().getUserId());
      Statement statement = conn.createStatement();
      statement.executeUpdate("update sessions set vis='"
          + StringEscapeUtils.escapeJava(Joiner.on(',').join(plotCols))
          + "'where id='" + cleanId(session.getUser().getUserId()) + "';");
    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static JSONObject getUserData(String userId) {
    try {
      makeSureSessionExistsInDB(userId);
      Statement statement = conn.createStatement();
      return new ResultTable(
          statement.executeQuery(
              "select display,result,vis from sessions where id='"
                  + cleanId(userId) + "';")).json();
    } catch (SQLException e) {
      log.error(e.getMessage());
      JSONObject obj = new JSONObject();
      obj.put("display",
          "There was an error retrieving the display text from the database");
      return obj;
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
