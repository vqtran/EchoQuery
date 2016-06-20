package echoquery.intents;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;
import com.google.common.base.Joiner;

import echoquery.querier.schema.ColumnName;
import echoquery.utils.Response;
import echoquery.utils.SlotUtil;
import echoquery.utils.VisualizationUtil;

public class ClearHandler implements IntentHandler {

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    List<String> matches = new ArrayList<>();
    VisualizationUtil.updatePlotColumns(matches, session);

    String description = "Your visualization has been cleared.";
    VisualizationUtil.updateDisplayText(description, session);
    return Response.say(description, session);
  }
}
