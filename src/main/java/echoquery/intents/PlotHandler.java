package echoquery.intents;

import java.util.ArrayList;
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
    List<ColumnName> axisColumns = new ArrayList<>();
    if (intent.getSlot(SlotUtil.PLOT_COLUMN_1).getValue() != null) {
      axisColumns.add(SlotUtil.parseColumnSlot(
          intent.getSlot(SlotUtil.PLOT_COLUMN_1).getValue()));
    }
    if (intent.getSlot(SlotUtil.PLOT_COLUMN_2).getValue() != null) {
      axisColumns.add(SlotUtil.parseColumnSlot(
          intent.getSlot(SlotUtil.PLOT_COLUMN_2).getValue()));
    }

    List<String> aggregations = new ArrayList<>();
    aggregations.add(SlotUtil.getFunction(
        intent.getSlot(SlotUtil.PLOT_AGGREGATION_1).getValue()));
    aggregations.add(SlotUtil.getFunction(
        intent.getSlot(SlotUtil.PLOT_AGGREGATION_2).getValue()));

    JSONObject columnData = new JSONObject(
        VisualizationUtil.getUserData(session.getUser().getUserId())
            .getJSONArray("sessions.result")
            .getString(0));

    List<String> matches = matchColumnsToPlot(axisColumns, columnData);

    VisualizationUtil.updatePlotColumns(matches, session);

    String description = "";
    switch (matches.size()) {
      case 0:
        description = "I'm not sure what columns to plot, please try again.";
        break;
      case 1:
        description = "Here's a histogram for "
            + Joiner.on(" ").join(matches.get(0).split("\\.")) + ".";
        break;
      case 2:
        description = "Here's a heatmap for "
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

  public static List<String> matchColumnsToPlot(
      List<ColumnName> axisColumns,
      List<String> aggregations,
      JSONObject columnData) {

    List<String> matches = new ArrayList<>();
    for (ColumnName toMatch : axisColumns) {
      for (String column : columnData.keySet()) {
        String aggregation = column.replaceAll("\\([^)]*\\)", "");
        if (aggregation.equals(column)) {
          aggregation = null;
        }
        String[] tableColumn = aggregation == null ? column.split("\\.")
            : column.replaceFirst(aggregation, "").replaceAll("[()]", "")
                .split("\\.");

        if (toMatch.getTable().isPresent()
            && toMatch.getTable().get().equals(tableColumn[0])
            && toMatch.getColumn().get().equals(tableColumn[1])) {
          matches.add(column);
          break;
        }
        if (!toMatch.getTable().isPresent()
            && toMatch.getColumn().get().equals(tableColumn[1])) {
          matches.add(column);
          break;
        }
      }
    }

    return matches;
  }
}
