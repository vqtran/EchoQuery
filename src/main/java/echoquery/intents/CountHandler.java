package echoquery.intents;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;

import echoquery.utils.Response;
import echoquery.utils.SlotNames;
import echoquery.utils.TranslationUtils;

public class CountHandler implements IntentHandler {

  private final Connection conn;
  private final Logger log;

  public CountHandler(Connection conn, Logger log) {
    this.conn = conn;
    this.log = log;
  }

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    try {
      Statement statement = conn.createStatement();
      String table = intent.getSlot(SlotNames.TABLE_NAME).getValue();
      ResultSet result =
          statement.executeQuery("select count(*) from " + table);
      result.first();
      return Response.say(
          "There are " + TranslationUtils.convert(result.getInt(1))
          + " rows in the " + table + " table.");

    } catch (SQLException e) {
      log.info("StatementCreationError: " + e.getMessage());
      return Response.say("There was an error querying the database.");
    }
  }
}
