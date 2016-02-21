package echoquery.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.json.JSONObject;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.base.Optional;

import echoquery.sql.joins.JoinRecipe;
import echoquery.utils.ResultSetConverter;
import echoquery.utils.SlotUtil;
import echoquery.utils.TranslationUtils;

/**
 * The QueryResult class contains a status of a query request and a message for
 * the end user.
 */

public class QueryResult {
  public enum Status {
    SUCCESS,
    REPAIR_REQUEST,
    FAILURE
  }
  private final Status status;
  private final String message;
  private final JSONObject data;

  public QueryResult(Status status, String message) {
    this.status = status;
    this.message = message;
    this.data = null;
  }

  public QueryResult(Status status, String message, JSONObject data) {
    this.status = status;
    this.message = message;
    this.data = data;
  }

  public Status getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  public JSONObject getData() {
    return data;
  }

  /**
   * Builds a QueryResult out of a JDBC result set, and original request object.
   * @param inferrer
   * @param request
   * @param result
   * @return A successful result where the message is the result of the query
   *    is in end-user natural language.
   */
  public static QueryResult of(
      SchemaInferrer inferrer, QueryRequest request, ResultSet result) {
    // Extract the results from the ResultSet.
    final Optional<Double> singleValue;
    List<Entry<String, Double>> groupByValues = new ArrayList<>();
    JSONObject json;
    try {
      json = ResultSetConverter.convert(result);
      if (request.getSelectAll().isPresent()) {
        return new QueryResult(Status.SUCCESS,
            "There are "
                + TranslationUtils.convert(
                    json.getJSONArray(json.keys().next()).length())
                + " rows in the get request.", json);
      } else if (request.getGroupByColumn().getColumn().isPresent()) {
        // If there was a group by, we expect two columns and many rows.
        result.first();
        // Add all of them to the list of entries.
        while (!result.isAfterLast()) {
          groupByValues.add(new AbstractMap.SimpleEntry<>(
              result.getString(1), result.getDouble(2)));
          result.next();
        }
        singleValue = Optional.absent();
      } else {
        // Otherwise just populate the single value.
        result.first();
        singleValue = Optional.fromNullable(result.getDouble(1));
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }

    return new QueryResult(
        Status.SUCCESS,
        translateResults(inferrer, request, singleValue, groupByValues),
        json);
  }

  /**
   * Translates request and parsed results into a natural language String.
   * @param inferrer
   * @param request
   * @param singleValue Empty if there's a group by, not otherwise.
   * @param groupByValues
   * @return A natural language string conveying the results.
   */
  public static String translateResults(
      SchemaInferrer inferrer,
      QueryRequest request,
      Optional<Double> singleValue,
      List<Entry<String, Double>> groupByValues) {
    /**
     * Visit each node in the tree and convert it to natural language. Results
     * in a sentence form of the query.
     */
    StringBuilder translation = new StringBuilder();

    /**
     * The boolean here signifies when to include the contents. Right now
     * is included except for QualifiedNameReferences in select
     * clauses (as they correspond to our internal book-keeping of group-bys).
     */
    AstVisitor<Void, Boolean> translator =
        new DefaultTraversalVisitor<Void, Boolean>() {

      @Override
      protected Void visitQuerySpecification(
          QuerySpecification node, Boolean capture) {
        // By default do not capture any qualified names in select clauses.
        process(node.getSelect(), false);

        if (node.getFrom().isPresent()) {
          // Use what the user referenced, ignoring any complicated join
          // inference we did to get it to work.
          translation.append("in the ")
              .append(request.getFromTable().get())
              .append(" table");
        }
        if (node.getWhere().isPresent()) {
            translation.append(" where ");
            process(node.getWhere().get(), true);
        }
        for (GroupingElement groupingElement : node.getGroupBy()) {
            process(groupingElement, true);
        }
        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), true);
        }
        for (SortItem sortItem : node.getOrderBy()) {
            process(sortItem, true);
        }
        return null;
      }

      @Override
      public Void visitFunctionCall(FunctionCall node, Boolean capture) {
        String aggregation = request.getAggregationFunc().get();
        if (aggregation.equals("COUNT")) {
          double value;
          if (singleValue.isPresent()) {
            value = singleValue.get();
          } else {
            value = groupByValues.get(0).getValue();
          }
          if (value == 1) {
            translation.append("There is ")
              .append(TranslationUtils.convert(value))
              .append(" row ");
          } else {
            translation.append("There are ")
              .append(TranslationUtils.convert(value))
              .append(" rows ");
          }
        } else {
          translation.append("The ")
              .append(SlotUtil.aggregationFunctionToEnglish(aggregation))
              .append(" of the ");
          // Only accept qualified names in select clauses that belong to a
          // aggregation function.
          process(node.getArguments().get(0), true);
          translation.append(" column ");
        }
        return null;
      }

      @Override
      public Void visitLogicalBinaryExpression(
          LogicalBinaryExpression node, Boolean capture) {
        process(node.getLeft(), true);
        switch (node.getType()) {
          case AND:
            translation.append(" and ");
            break;
          case OR:
            translation.append(" or ");
            break;
        }
        process(node.getRight(), true);
        return null;
      }

      @Override
      public Void visitComparisonExpression(
          ComparisonExpression node, Boolean capture) {
        process(node.getLeft(), true);
        translation.append(" is")
            .append(SlotUtil.comparisonTypeToEnglish(node.getType()));
        process(node.getRight(), true);
        return null;
      }

      @Override
      public Void visitQualifiedNameReference(
          QualifiedNameReference node, Boolean capture) {
        if (capture) {
          // If it has a table name on it, decide if we need to include it in
          // the translation.
          if (node.getName().getPrefix().isPresent()) {
            // Include it in the translation only if it would be ambiguous
            // what table the column is coming from if we didn't.
            if (isAmbiguousWithoutTable(
                node.getSuffix().toString(), inferrer)) {
              String prefix = node.getName().getPrefix().get().toString();
              // Remove any trailing s's for grammar's sake.
              if (prefix.endsWith("s")) {
                prefix = prefix.substring(0, prefix.length()-1);
              }
              translation.append(prefix).append(" ");
            }
          }
          translation.append(node.getSuffix());
        }
        return null;
      }

      @Override
      public Void visitStringLiteral(StringLiteral node, Boolean capture) {
        translation.append(node.getValue());
        return null;
      }

      @Override
      public Void visitLongLiteral(LongLiteral node, Boolean capture) {
        translation.append(TranslationUtils.convert(node.getValue()));
        return null;
      }
    };
    translator.process(request.getQuery(), true);

    /**
     * At this point the body of the query has been translated, but we need to
     * append one or more results to the end now.
     */

    // If it's count and it only had a single value, we're all set.
    if (request.getAggregationFunc().get().equals("COUNT")) {
      // Otherwise if its count and a group by, we then have to say the group of
      // the first result we already stated, as well as add on the rest of the
      // results by group.
      if (!singleValue.isPresent()) {
        translation.append(" for the ");
        if (isAmbiguousWithoutTable(
            request.getGroupByColumn().getColumn().get(), inferrer)) {
          translation.append(request.getContext().getGroupByPrefix().get())
              .append(" ");
        }
        translation.append(request.getGroupByColumn().getColumn().get())
            .append(" ")
            .append(groupByValues.get(0).getKey());
        for (int i = 1; i < groupByValues.size(); i++) {
          translation.append(", ");
          if (i == groupByValues.size() - 1) {
            translation.append("and ");
          }
          translation
              .append(TranslationUtils.convert(groupByValues.get(i).getValue()))
              .append((groupByValues.get(i).getValue() == 1)
                  ? " row for " : " rows for ")
              .append(groupByValues.get(i).getKey());
        }
      }
    } else {
      // For all other aggregations, if there's only a single value we simply
      // tack it on.
      if (singleValue.isPresent()) {
        translation.append(" is ")
            .append(TranslationUtils.convert(singleValue.get()));
      }
      // Otherwise we have to tack on all of the remaining values.
      for (int i = 0; i < groupByValues.size(); i++) {
        if (i == 0) {
          translation.append(" is ")
              .append(TranslationUtils.convert(groupByValues.get(i).getValue()))
              .append(" for the ");
          // Include what table group by belonged to only if ambiguous
          // otherwise.
          if (isAmbiguousWithoutTable(
              request.getGroupByColumn().getColumn().get(), inferrer)) {
            translation.append(request.getContext().getGroupByPrefix().get())
                .append(" ");
          }
          translation.append(request.getGroupByColumn().getColumn().get())
              .append(" ")
              .append(groupByValues.get(i).getKey());
        } else {
          translation.append(", ");
          if (i == groupByValues.size() - 1) {
            translation.append("and ");
          }
          translation
              .append(TranslationUtils.convert(groupByValues.get(i).getValue()))
              .append(" for ")
              .append(groupByValues.get(i).getKey());
        }
      }
    }
    translation.append(".");
    return translation.toString();
  }

  /**
   * True if column name belongs to more than one table.
   * @param column
   * @param inferrer
   * @return
   */
  private static boolean isAmbiguousWithoutTable(
      String column, SchemaInferrer inferrer) {
    Set<String> containingTables = inferrer.getColumnsToTable().get(column);
    return containingTables.size() > 1;
  }

  /**
   * Creates a QueryResult for an InvalidJoinRecipe.
   * @param request
   * @param invalid
   * @return
   */
  public static QueryResult of(
      QueryRequest request, JoinRecipe invalid) {
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
        return new QueryResult(Status.REPAIR_REQUEST, askWhichTable(
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
          return new QueryResult(Status.REPAIR_REQUEST, askWhichTable(
              "where " + col + " is"
                  + SlotUtil.comparisonTypeToEnglish(comparator) + val,
              invalid.getPossibleTables(),
              col));
        }
      }

      if (request.getGroupByColumn().getColumn().isPresent()
          && !invalid.getContext().getGroupByPrefix().isPresent()) {
        String col = request.getGroupByColumn().getColumn().get();
        return new QueryResult(Status.REPAIR_REQUEST, askWhichTable(
            "for each " + col, invalid.getPossibleTables(), col));
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

