package echoquery;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

import echoquery.intents.PlotHandler;
import echoquery.querier.ResultTable;
import echoquery.querier.schema.ColumnName;

public class PlotHandlerTest {
  private static JSONObject columnData;

  @BeforeClass
  public static void loadColumnData() {
    try {
      Statement statement =
          SingletonConnections.getDataInstance().createStatement();
      columnData = new ResultTable(statement.executeQuery(
          "select patients.name,patients.age,avg(patients.name),count(*)"
              + " from patients group by patients.name limit 5;")).json();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testMatchColumnsToPlot_singleAxisFullInfo() {
    List<ColumnName> toMatch = Arrays.asList(
        new ColumnName("patients", "name"));
    List<String> matches = PlotHandler.matchColumnsToPlot(toMatch, columnData);
    System.out.println(matches);
    assertTrue(Arrays.equals(matches.toArray(), new String[]{"patients.name"}));
  }

  @Test
  public void testMatchColumnsToPlot_singleAxisLessInfo() {
    List<ColumnName> toMatch = Arrays.asList(
        new ColumnName(null, "name"));
    List<String> matches = PlotHandler.matchColumnsToPlot(toMatch, columnData);
    assertTrue(Arrays.equals(matches.toArray(), new String[]{"patients.name"}));
  }

}
