package echoquery;

import java.util.HashSet;
import java.util.Set;

import com.amazon.speech.speechlet.Speechlet;
import com.amazon.speech.speechlet.lambda.SpeechletRequestStreamHandler;

/**
 * Request handler for AWS Lambda deployment.
 */
public class EchoQuerySpeechletRequestStreamHandler
  extends SpeechletRequestStreamHandler {

  private static final Set<String> supportedApplicationIds;
  static {
    supportedApplicationIds = new HashSet<String>();
    // Gabe's
    supportedApplicationIds.add(
        "amzn1.echo-sdk-ams.app.f705b0c1-42a2-4c94-9e48-50aed83b2310");
    // Vinh's
    supportedApplicationIds.add(
        "amzn1.echo-sdk-ams.app.3c94817f-47ab-4894-8eb3-34945ef13cfe");
  }

  public EchoQuerySpeechletRequestStreamHandler() {
    super(new EchoQuerySpeechlet(), supportedApplicationIds);
  }

  public EchoQuerySpeechletRequestStreamHandler(
      Speechlet speechlet, Set<String> supportedApplicationIds) {
    super(speechlet, supportedApplicationIds);
  }
}