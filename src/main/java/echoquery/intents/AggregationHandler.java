package echoquery.intents;

import java.io.IOException;
import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;

import echoquery.sql.Querier;
import echoquery.sql.QueryRequest;
import echoquery.sql.QueryResult;
import echoquery.utils.Response;
import echoquery.utils.Serializer;
import echoquery.utils.SessionUtil;

/**
 * Where all aggregation query requests should be funneled into, regardless
 * from end-user or another intent.
 */
public class AggregationHandler implements IntentHandler {

  private static final Logger log =
      LoggerFactory.getLogger(AggregationHandler.class);

  private final Querier querier;

  public AggregationHandler(Connection conn) {
    this.querier = new Querier(conn);
  }

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    return respond(QueryRequest.of(intent), session);
  }

  /**
   * Generic version of respond method to be called directly by other intents.
   * @param request
   * @param session
   * @return
   */
  public SpeechletResponse respond(QueryRequest request, Session session) {
    // Save the current request object to session.
    try {
      session.setAttribute(
          SessionUtil.REQUEST_ATTRIBUTE, Serializer.serialize(request));
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      return Response.unexpectedError();
    }

    // Execute the request and handle results.
    QueryResult result = querier.execute(request);
    if (result.getStatus() == QueryResult.Status.REPAIR_REQUEST) {
      return Response.ask(result.getMessage(), result.getMessage());
    } else {
      return Response.say(result.getMessage());
    }
  }

  /**
   * Exposed for testing purposes - SpeechletResponse is impossible to inspect.
   */
  public String getResponseInEnglish(Intent intent) {
    return querier.execute(QueryRequest.of(intent)).getMessage();
  }
}
