package echoquery.querier;

import java.util.List;

import org.json.JSONObject;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.google.common.base.Optional;

import echoquery.querier.infer.JoinRecipe;
import echoquery.utils.SlotUtil;

/**
 * The QueryResult class contains a status of a query request and a message for
 * the end user.
 */

public class QueryResult {
  public enum Status {
    SUCCESS,
    FAILURE,
    CLARIFICATION_NEEDED,
  }

  public enum Problem {
    AMBIGUOUS_TABLE_FOR_COLUMN,
  }

  private final Status status;
  private final Problem problem;
  private final String message;
  private final JSONObject data;

  public QueryResult(Status status, String message) {
    this.status = status;
    this.problem = null;
    this.message = message;
    this.data = null;
  }

  public QueryResult(Status status, Problem problem, String message) {
    this.status = status;
    this.problem = problem;
    this.message = message;
    this.data = null;
  }

  public QueryResult(Status status, String message, JSONObject data) {
    this.status = status;
    this.problem = null;
    this.message = message;
    this.data = data;
  }

  public Status getStatus() {
    return status;
  }

  public Problem getProblem() {
    return problem;
  }

  public String getMessage() {
    return message;
  }

  public JSONObject getData() {
    return data;
  }

  /**
   * Creates a QueryResult for an InvalidJoinRecipe.
   * @param request
   * @param invalid
   * @return
   */
  public static QueryResult of(QueryRequest request, JoinRecipe invalid) {
    // There are two cases for invalid join recipes: ambiguous where a column
    // came from, or the column doesn't have a foreign key connecting it at all.
    switch (invalid.getReason()) {

    // If ambiguous, we're going to return a result that will ask the user for
    // which one she wants.
    case AMBIGUOUS_TABLE_FOR_COLUMN:

      // Find the first ambiguous column and ask. Order here is important:
      // aggregation, where clauses in order, group by.

      if (request.getAggregationColumn().getColumn().isPresent()
          && !invalid.getContext().getAggregationPrefix().isPresent()) {
        String col = request.getAggregationColumn().getColumn().get();
        return new QueryResult(
            Status.CLARIFICATION_NEEDED,
            Problem.AMBIGUOUS_TABLE_FOR_COLUMN,
            askWhichTable(
                SlotUtil.aggregationFunctionToEnglish(
                    request.getAggregationFunc().get()) + " of " + col,
                invalid.getPossibleTables(),
                col));
      }

      List<Optional<String>> comparisonPrefixes =
          invalid.getContext().getComparisons();
      for (int i = 0; i < comparisonPrefixes.size(); i++) {
        if (!comparisonPrefixes.get(i).isPresent()) {
          String col = request.getComparisonColumns().get(i).getColumn().get();
          String val = request.getComparisonValues().get(i).get();
          ComparisonExpression.Type comparator =
              request.getComparators().get(i).get();
          return new QueryResult(
              Status.CLARIFICATION_NEEDED,
              Problem.AMBIGUOUS_TABLE_FOR_COLUMN,
              askWhichTable(
                  "where " + col + " is"
                      + SlotUtil.comparisonTypeToEnglish(comparator) + val,
                  invalid.getPossibleTables(),
                  col));
        }
      }

      if (request.getGroupByColumn().getColumn().isPresent()
          && !invalid.getContext().getGroupByPrefix().isPresent()) {
        String col = request.getGroupByColumn().getColumn().get();
        return new QueryResult(
            Status.CLARIFICATION_NEEDED,
            Problem.AMBIGUOUS_TABLE_FOR_COLUMN,
            askWhichTable("for each " + col, invalid.getPossibleTables(), col));
      }

      return null;

    // For now missing foreign keys will just be notified.
    case MISSING_FOREIGN_KEY:
      return new QueryResult(Status.FAILURE,
          invalid.getInvalidColumn() + " doesn't seem to be a column related "
              + "to the table you referenced.");
    }
    return null;
  }

  /**
   * @param partOfQuery
   * @param possibleTables
   * @param col
   * @return
   */
  private static String askWhichTable(
      String partOfQuery, List<String> possibleTables, String col) {
    String message = "By "
        + partOfQuery + ", are you referring to ";
    for (int i = 0; i < possibleTables.size() - 1; i++) {
      message += possibleTables.get(i) + " " + col + ", ";
    }
    message += "or "
        + possibleTables.get(possibleTables.size() - 1) + " " + col + "?";
    return message;
  }

}

