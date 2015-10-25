package echoquery.intents;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;

/**
 * Intent handler respond to matches in requests routed their way from the
 * onIntent method.
 */
public interface IntentHandler {

  /**
   * Respond with appropriate message/prompting.
   * @param intent
   * @param session
   * @return
   */
  public SpeechletResponse respond(Intent intent, Session session);
}
