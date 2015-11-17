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
import echoquery.sql.QueryResult;
import echoquery.sql.SingletonConnection;
import echoquery.utils.Response;

public class AggregationHandler implements IntentHandler {

  private static final Logger log =
      LoggerFactory.getLogger(AggregationHandler.class);
  private static final Querier querier =
      new Querier(SingletonConnection.getInstance());

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    try {
      Query query = QueryBuilder.of(intent).build();
      QueryResult result = querier.execute(query);

      return Response.say(result.toEnglish());

    } catch (SQLException e) {
      log.info("StatementCreationError: " + e.getMessage());
      return Response.say("There was an error querying the database.");
    }
  }
}
