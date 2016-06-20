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

public class PlotHandler implements IntentHandler {

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    List<ColumnName> axes = new ArrayList<>();

    if (intent.getSlot(SlotUtil.PLOT_COLUMN_1).getValue() != null) {
      axes.add(SlotUtil.parseColumnSlot(
          intent.getSlot(SlotUtil.PLOT_COLUMN_1).getValue()));
    }
    if (intent.getSlot(SlotUtil.PLOT_COLUMN_2).getValue() != null) {
      axes.add(SlotUtil.parseColumnSlot(
          intent.getSlot(SlotUtil.PLOT_COLUMN_2).getValue()));
    }

    List<String> matches = new ArrayList<>();
    JSONObject columnData = new JSONObject(VisualizationUtil.getUserData(
        session.getUser().getUserId()).getJSONArray("sessions.result")
        .getString(0));
    for (ColumnName axis : axes) {
      for (String column : columnData.keySet()) {
        System.out.println(column);
        String[] tableColumn = column.split("\\.");
        System.out.println(Arrays.toString(tableColumn));
        if (axis.getTable().isPresent()
            && axis.getTable().get().equals(tableColumn[0])
            && axis.getColumn().get().equals(tableColumn[1])) {
          matches.add(column);
          break;
        }
        if (!axis.getTable().isPresent()
            && axis.getColumn().get().equals(tableColumn[1])) {
          matches.add(column);
          break;
        }
      }
    }

    VisualizationUtil.updatePlotColumns(matches, session);

    String description = "";
    switch (matches.size()) {
      case 0:
        description = "I am not sure what columns to plot, please try again.";
        break;
      case 1:
        description = "Here is a histogram of "
            + Joiner.on(" ").join(matches.get(0).split("\\.")) + ".";
        break;
      case 2:
        description = "Here is a heatmap of "
            + Joiner.on(" ").join(matches.get(0).split("\\.")) + " versus "
            + Joiner.on(" ").join(matches.get(1).split("\\.")) + ".";
        break;
    }

    if (description.isEmpty()) {
      return Response.unexpectedError(session);
    }

    VisualizationUtil.updateDisplayText(description, session);
    return Response.say(description, session);
  }
}
