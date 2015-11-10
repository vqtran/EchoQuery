package echoquery.intents;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;

import echoquery.sql.SingletonConnection;
import echoquery.utils.Response;
import echoquery.utils.SlotNames;
import echoquery.utils.TranslationUtils;

public class CountHandler implements IntentHandler {

  private static final Logger log =
      LoggerFactory.getLogger(CountHandler.class);

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    try {
      String table = intent.getSlot(SlotNames.TABLE_NAME).getValue();
      String column = intent.getSlot(SlotNames.COLUMN_NAME).getValue();
      String colVal= intent.getSlot(SlotNames.COLUMN_VALUE).getValue();
      String colNum = intent.getSlot(SlotNames.COLUMN_NUMBER).getValue();
      String equals = intent.getSlot(SlotNames.EQUALS).getValue();
      String greater = intent.getSlot(SlotNames.GREATER_THAN).getValue();
      String less = intent.getSlot(SlotNames.LESS_THAN).getValue();

      // TODO(vqtran): Pull out this parsing into a reusable Parser class that
      // parses it into some sort of abstract syntax / handles error checking.
      String query = "SELECT COUNT(*) FROM " + table;
      String match = null;
      if (column != null) {
        match = (colVal == null) ? colNum : colVal;
        String comparison = "";
        if (equals != null) {
          comparison = "=";
        }
        if (greater != null) {
          comparison = ">";
        }
        if (less != null) {
          comparison = "<";
        }
        query += " WHERE " + column + " " + comparison + " '" + match + "'";
      }

      Statement statement = SingletonConnection.getInstance().createStatement();
      ResultSet result = statement.executeQuery(query);
      result.first();

      // TODO(vqtran): Pull this out into a response generator class that takes
      // the result and the Parser's abstract syntax object.
      String response = "There ";
      if (result.getInt(1) == 1) {
        response +=
            "is " + TranslationUtils.convert(result.getInt(1)) + " row";
      } else {
        response +=
            "are " + TranslationUtils.convert(result.getInt(1)) + " rows";
      }
      response += " in the " + table + " table";
      if (column != null) {
        response += " where the value in the " + column + " column is ";
        if (equals != null) {
          response += "equal to ";
        }
        if (greater != null) {
          response += "greater than ";
        }
        if (less != null) {
          response += "less than ";
        }
        if (colVal == null) {
          response += TranslationUtils.convert(Integer.parseInt(match));
        } else {
          response += colVal;
        }
      }
      response += ".";

      return Response.say(response);

    } catch (SQLException e) {
      log.info("StatementCreationError: " + e.getMessage());
      return Response.say("There was an error querying the database.");
    }
  }
}
