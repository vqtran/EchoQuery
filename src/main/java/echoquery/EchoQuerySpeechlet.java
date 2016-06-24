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

import echoquery.intents.ClarifyHandler;
import echoquery.intents.HelpHandler;
import echoquery.intents.IntentHandler;
import echoquery.intents.PlotHandler;
import echoquery.intents.ClearHandler;
import echoquery.intents.QueryHandler;
import echoquery.intents.RefineHandler;
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

  private QueryHandler queryHandler;
  private IntentHandler helpHandler;
  private IntentHandler clarifyHandler;
  private IntentHandler refineHandler;
  private IntentHandler plotHandler;
  private IntentHandler clearHandler;

  public EchoQuerySpeechlet() {
    super();
    helpHandler = new HelpHandler();
    queryHandler = new QueryHandler(SingletonConnections.getDataInstance());
    refineHandler = new RefineHandler(
        SingletonConnections.getDataInstance(), queryHandler);
    clarifyHandler = new ClarifyHandler(
        SingletonConnections.getDataInstance(), queryHandler);
    plotHandler = new PlotHandler();
    clearHandler = new ClearHandler();
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

    return Response.welcome(session);
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
      case "QueryIntent":
        return queryHandler.respond(intent, session);
      case "RefineIntent":
        return refineHandler.respond(intent, session);
      case "ClarifyIntent":
        return clarifyHandler.respond(intent, session);
      case "PlotIntent":
        return plotHandler.respond(intent, session);
      case "ClearIntent":
        return clearHandler.respond(intent, session);
      case "AMAZON.HelpIntent":
        return helpHandler.respond(intent, session);
      case "AMAZON.StopIntent":
        return Response.bye(session);
      case "AMAZON.CancelIntent":
        return Response.bye(session);
      case "AMAZON.NoIntent":
        return Response.bye(session);
      default:
        throw new SpeechletException("Invalid Intent: " + intentName);
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
