package echoquery.intents;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;

import echoquery.sql.Querier;
import echoquery.sql.QueryRequest;
import echoquery.sql.QueryResult;
import echoquery.utils.Response;

public class AggregationHandler implements IntentHandler {

  private static final Logger log =
      LoggerFactory.getLogger(AggregationHandler.class);
  private final Querier querier;

  public AggregationHandler(Connection conn) {
    this.querier = new Querier(conn);
  }

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    return Response.say(getResponseInEnglish(intent, session));
  }

  /**
   * Exposed for testing purposes - SpeechletResponse is impossible to inspect.
   */
  public String getResponseInEnglish(Intent intent, Session session) {
    return querier.execute(QueryRequest.of(intent)).getMessage();
  }
}
