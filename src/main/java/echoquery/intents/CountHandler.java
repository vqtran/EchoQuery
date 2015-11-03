package echoquery.intents;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;

import echoquery.sql.StarSchemaQuerier;
import echoquery.utils.Response;
import echoquery.utils.SlotNames;
import echoquery.utils.TranslationUtils;

public class CountHandler implements IntentHandler {

  private static final Logger log =
      LoggerFactory.getLogger(CountHandler.class);
  private static final StarSchemaQuerier querier = StarSchemaQuerier.getInstance();

  public CountHandler() {
  }

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    try {
      String table = intent.getSlot(SlotNames.TABLE_NAME).getValue();
      int count = querier.count(table);
      return Response.say(
          "There are " + TranslationUtils.convert(count)
          + " rows in the " + table + " table.");

    } catch (SQLException e) {
      log.info("StatementCreationError: " + e.getMessage());
      return Response.say("There was an error querying the database.");
    }
  }
}
