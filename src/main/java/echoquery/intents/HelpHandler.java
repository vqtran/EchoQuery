package echoquery.intents;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;

import echoquery.utils.Response;

public class HelpHandler implements IntentHandler {

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    String askOutput =
      "With EchoQuery, you can query"
          + " your database for aggregates, group bys, and order bys"
          + " For example, you could say,"
          + " How many sales did we have?";
    String repromptOutput = "What do you want to ask?";
    return Response.ask(askOutput, repromptOutput, session);

  }
}
