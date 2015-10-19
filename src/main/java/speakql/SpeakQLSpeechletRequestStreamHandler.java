package speakql;

import java.util.HashSet;
import java.util.Set;

import com.amazon.speech.speechlet.Speechlet;
import com.amazon.speech.speechlet.lambda.SpeechletRequestStreamHandler;

/**
 * Request handler for AWS Lambda deployment.
 */
public class SpeakQLSpeechletRequestStreamHandler
  extends SpeechletRequestStreamHandler {

  private static final Set<String> supportedApplicationIds;

  // TODO(vqtran): Make application id a system property because developers
  // probably should not share volatile development instances of Alexa skills.
  static {
    supportedApplicationIds = new HashSet<String>();
    supportedApplicationIds.add(
        "amzn1.echo-sdk-ams.app.f705b0c1-42a2-4c94-9e48-50aed83b2310");
  }

  public SpeakQLSpeechletRequestStreamHandler() {
    super(new SpeakQLSpeechlet(), supportedApplicationIds);
  }

  public SpeakQLSpeechletRequestStreamHandler(
      Speechlet speechlet, Set<String> supportedApplicationIds) {
    super(speechlet, supportedApplicationIds);
  }
}