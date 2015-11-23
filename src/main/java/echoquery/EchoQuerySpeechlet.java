package echoquery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.IntentRequest;
import com.amazon.speech.speechlet.LaunchRequest;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SessionEndedRequest;
import com.amazon.speech.speechlet.SessionStartedRequest;
import com.amazon.speech.speechlet.Speechlet;
import com.amazon.speech.speechlet.SpeechletException;
import com.amazon.speech.speechlet.SpeechletResponse;

import echoquery.intents.AggregationHandler;
import echoquery.intents.HelpHandler;
import echoquery.intents.IntentHandler;
import echoquery.sql.SingletonConnection;
import echoquery.utils.Response;

/**
 * Ask for certain pieces of information and SpeakQL will translate it into an
 * SQL query.
 */
public class EchoQuerySpeechlet implements Speechlet {
  /**
   * Statically initialize the logger, this goes to S3.
   */
  private static final Logger log =
      LoggerFactory.getLogger(EchoQuerySpeechlet.class);

  private IntentHandler aggregationHandler;
  private IntentHandler helpHandler;

  public EchoQuerySpeechlet() {
    super();
    aggregationHandler =
        new AggregationHandler(SingletonConnection.getInstance());
    helpHandler = new HelpHandler();
  }

  @Override
  public void onSessionStarted(
      final SessionStartedRequest request, final Session session)
      throws SpeechletException {
    log.info("onSessionStarted requestId={}, sessionId={}",
        request.getRequestId(), session.getSessionId());
  }

  @Override
  public SpeechletResponse onLaunch(
      final LaunchRequest request, final Session session)
      throws SpeechletException {
    log.info("onLaunch requestId={}, sessionId={}", request.getRequestId(),
        session.getSessionId());

    return Response.welcome();
  }

  @Override
  public SpeechletResponse onIntent(
      final IntentRequest request, final Session session)
      throws SpeechletException {
    log.info("onIntent requestId={}, sessionId={}", request.getRequestId(),
        session.getSessionId());

    Intent intent = request.getIntent();
    String intentName = intent.getName();

    // Route the intent to the proper handlers.
    switch(intentName) {
      case "AggregationIntent":
        return aggregationHandler.respond(intent, session);
      case "HelpIntent":
        return helpHandler.respond(intent, session);
      case "FinishIntent":
        return Response.bye();
      default:
        throw new SpeechletException("Invalid Intent");
    }
  }

  @Override
  public void onSessionEnded(
      final SessionEndedRequest request, final Session session)
      throws SpeechletException {
    log.info("onSessionEnded requestId={}, sessionId={}",
        request.getRequestId(), session.getSessionId());
  }
}
