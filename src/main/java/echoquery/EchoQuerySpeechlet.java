package echoquery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.slu.Slot;
import com.amazon.speech.speechlet.IntentRequest;
import com.amazon.speech.speechlet.LaunchRequest;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SessionEndedRequest;
import com.amazon.speech.speechlet.SessionStartedRequest;
import com.amazon.speech.speechlet.Speechlet;
import com.amazon.speech.speechlet.SpeechletException;
import com.amazon.speech.speechlet.SpeechletResponse;
import com.amazon.speech.ui.SsmlOutputSpeech;
import com.amazon.speech.ui.Reprompt;

/**
 * Ask for certain pieces of information and SpeakQL will translate it into an
 * SQL query.
 */
public class EchoQuerySpeechlet implements Speechlet {
  private static final Logger log =
      LoggerFactory.getLogger(EchoQuerySpeechlet.class);
  
  private static final Connection conn = getConnection();

  /**
   * Constant defining session attribute key for the intent slot key for the
   * date of events.
   */
  private static final String SLOT_TABLE = "TableName";

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

    return getWelcomeResponse();
  }

  public static Connection getConnection(){
    String connectionUrl = "jdbc:mysql://speechql.cutq0x5qwogl.us-east-1.rds.amazonaws.com:3306/bestbuy";
    String dbUser = EchoQueryCredentials.dbUser;
    String dbPwd = EchoQueryCredentials.dbPwd;
    Connection conn = null;

    try {
      conn = (Connection) DriverManager.getConnection(connectionUrl, dbUser, dbPwd);
      log.info("conn Available");
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      log.info("fetch motion error"+e.getLocalizedMessage());
    }

    return conn;
  }   

  @Override
  public SpeechletResponse onIntent(
      final IntentRequest request, final Session session)
      throws SpeechletException {
    log.info("onIntent requestId={}, sessionId={}", request.getRequestId(),
        session.getSessionId());

    Intent intent = request.getIntent();
    String intentName = intent.getName();

    if ("CountIntent".equals(intentName)) {
      return CountIntent(intent, session);
    } else if ("HelpIntent".equals(intentName)) {
      // Create the plain text output.
      String speechOutput =
        "With EchoQuery you can query"
            + " your database for aggreagtes, group bys, and order bys"
            + " For example, you could say,"
            + " How many sales did we have?";

      String repromptText = "What do you want to ask?";

      return newAskResponse(
          "<speak>" + speechOutput + "</speak>",
          "<speak>" + repromptText + "</speak>");
    } else if ("FinishIntent".equals(intentName)) {
      SsmlOutputSpeech outputSpeech = new SsmlOutputSpeech();
      outputSpeech.setSsml("<speak> Goodbye </speak>");
      return SpeechletResponse.newTellResponse(outputSpeech);
    } else {
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

  /**
   * Function to handle the onLaunch skill behavior.
   * 
   * @return SpeechletResponse object with voice/card response to return to the
   *     user
   */
  private SpeechletResponse getWelcomeResponse() {
    String speechOutput = "Echo Query. What do you want?";
    // If the user either does not reply to the welcome message or says
    // something that is not understood, they will be prompted again with this 
    // text.
    String repromptText = "With Echo Query, the world is your oyster";

    return newAskResponse("<speak>" + speechOutput + "</speak>",
        "<speak>" + repromptText + "</speak>");
  }

  /**
   * Prepares the speech to reply to the user. Obtain events from Wikipedia for
   * the date specified by the user (or for today's date, if no date is
   * specified), and return those events in both speech and SimpleCard format.
   * 
   * @param intent The intent object which contains the date slot
   * @param session The session object
   * @return SpeechletResponse object with voice/card response to return to the
   *     user.
   */
  private SpeechletResponse CountIntent(Intent intent, Session session) {
    SsmlOutputSpeech outputSpeech = new SsmlOutputSpeech();
    try {
      Statement statement = conn.createStatement();
      Slot tempSlot = intent.getSlot(SLOT_TABLE);
      log.info("Got Slot = {}", tempSlot.toString());
      String table = tempSlot.getValue();
      ResultSet result = statement.executeQuery("select count(*) from " + table);

      // Create the plain text output
      result.first();
      String speechOutput = "There are " + TranslationUtils.convert(result.getInt(1)) + " rows in table " + table;
      outputSpeech.setSsml("<speak>" + speechOutput + "</speak>");
    } catch (SQLException e) {
      log.info("StatementCreationError: " + e.getMessage());
      outputSpeech.setSsml("<speak> There was an error querying the database </speak>");
    }

    return SpeechletResponse.newTellResponse(outputSpeech);
  }

  /**
   * Wrapper for creating the Ask response from the input strings.
   * 
   * @param stringOutput The output to be spoken
   * @param repromptText The reprompt for if the user doesn't reply or is
   *     misunderstood.
   * @return SpeechletResponse the speechlet response
   */
  private SpeechletResponse newAskResponse(
      String stringOutput, String repromptText) {
    SsmlOutputSpeech outputSpeech = new SsmlOutputSpeech();
    outputSpeech.setSsml(stringOutput);
    SsmlOutputSpeech repromptOutputSpeech = new SsmlOutputSpeech();
    repromptOutputSpeech.setSsml(repromptText);
    Reprompt reprompt = new Reprompt();
    reprompt.setOutputSpeech(repromptOutputSpeech);
    return SpeechletResponse.newAskResponse(outputSpeech, reprompt);
  }
}