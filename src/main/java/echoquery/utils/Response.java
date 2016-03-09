package echoquery.utils;

import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;
import com.amazon.speech.ui.Reprompt;
import com.amazon.speech.ui.SsmlOutputSpeech;

public final class Response {

  /**
   * Have Alexa say something.
   * @param message What to say, without speak tags.
   * @return SpeechletResponse
   */
  public static SpeechletResponse say(String message, Session session) {
    return ask(message, "Is there anything else?", session);
  }

  /**
   * Have Alexa ask something, and reprompt if there was no answer.
   *
   * @param askOutput The initial question without speak tags.
   * @param repromptOutput The reprompt statement without speak tags.
   * @return SpeechletResponse
   */
  public static SpeechletResponse ask(String askOutput, String repromptOutput,
      Session session) {
    SsmlOutputSpeech outputSpeech = new SsmlOutputSpeech();
    outputSpeech.setSsml("<speak>" + askOutput + "</speak>");
    SsmlOutputSpeech repromptOutputSpeech = new SsmlOutputSpeech();
    repromptOutputSpeech.setSsml("<speak>" + repromptOutput + "</speak>");
    Reprompt reprompt = new Reprompt();
    reprompt.setOutputSpeech(repromptOutputSpeech);
    return SpeechletResponse.newAskResponse(outputSpeech, reprompt);
  }

  /**
   * Welcome prompt for the user.
   * @return SpeechletResponse
   */
  public static SpeechletResponse welcome(Session session) {
    String speechOutput = "Echo Query. What do you want?";
    String repromptOutput = "With Echo Query, the world is your oyster!";
    return ask(speechOutput, repromptOutput, session);
  }

  /**
   * Bye message for the user.
   * @return SpeechletResponse
   */
  public static SpeechletResponse bye(Session session) {
    SsmlOutputSpeech outputSpeech = new SsmlOutputSpeech();
    outputSpeech.setSsml("<speak></speak>");
    return SpeechletResponse.newTellResponse(outputSpeech);
  }

  /**
   * Standard unexpected error message.
   * @return SpeechletResponse
   */
  public static SpeechletResponse unexpectedError(Session session) {
    return Response.say(
        "There was an unexpected error. Please try again.", session);
  }
}