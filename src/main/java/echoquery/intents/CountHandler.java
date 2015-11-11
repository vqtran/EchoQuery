package echoquery.intents;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;
import com.facebook.presto.sql.tree.Query;

import echoquery.sql.Querier;
import echoquery.sql.QueryBuilder;
import echoquery.sql.SingletonConnection;
import echoquery.utils.Response;

public class CountHandler implements IntentHandler {

  private static final Logger log =
      LoggerFactory.getLogger(CountHandler.class);
  private static final Querier querier =
      new Querier(SingletonConnection.getInstance());

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    try {
      Query query = QueryBuilder.from(intent).build();
      int result = querier.execute(query);

      // TODO(vqtran): Pull this out into a response generator class that takes
      // the result and the Parser's abstract syntax object.
//      String response = "There ";
//      if (result.getInt(1) == 1) {
//        response +=
//            "is " + TranslationUtils.convert(result.getInt(1)) + " row";
//      } else {
//        response +=
//            "are " + TranslationUtils.convert(result.getInt(1)) + " rows";
//      }
//      response += " in the " + table + " table";
//      if (column != null) {
//        response += " where the value in the " + column + " column is ";
//        if (equals != null) {
//          response += "equal to ";
//        }
//        if (greater != null) {
//          response += "greater than ";
//        }
//        if (less != null) {
//          response += "less than ";
//        }
//        if (colVal == null) {
//          response += TranslationUtils.convert(Integer.parseInt(match));
//        } else {
//          response += colVal;
//        }
//      }
//      response += ".";

      return Response.say(result + "");

    } catch (SQLException e) {
      log.info("StatementCreationError: " + e.getMessage());
      return Response.say("There was an error querying the database.");
    }
  }
}
