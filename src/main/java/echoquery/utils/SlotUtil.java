package echoquery.utils;

import java.util.Arrays;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;

import echoquery.querier.schema.ColumnName;
import echoquery.querier.schema.ColumnType;

/**
 * TODO(vqtran): Refactor this class, it's a kitchen sink!
 */

public final class SlotUtil {
  public final static String TABLE_NAME = "TableName";

  public final static String FUNC = "Func";
  public final static String AGGREGATION_COLUMN = "AggregationColumn";

  public final static String COMPARISON_COLUMN_1 = "ComparisonColumnOne";
  public final static String COMPARATOR_1 = "ComparatorOne";
  public final static String COLUMN_VALUE_1 = "ColumnValueOne";
  public final static String COLUMN_NUMBER_1 = "ColumnNumberOne";

  public final static String BINARY_LOGIC_OP_1 = "BinaryLogicOpOne";

  public final static String COMPARISON_COLUMN_2 = "ComparisonColumnTwo";
  public final static String COMPARATOR_2 = "ComparatorTwo";
  public final static String COLUMN_VALUE_2 = "ColumnValueTwo";
  public final static String COLUMN_NUMBER_2 = "ColumnNumberTwo";

  public final static String BINARY_LOGIC_OP_2 = "BinaryLogicOpTwo";

  public final static String COMPARISON_COLUMN_3 = "ComparisonColumnThree";
  public final static String COMPARATOR_3 = "ComparatorThree";
  public final static String COLUMN_VALUE_3 = "ColumnValueThree";
  public final static String COLUMN_NUMBER_3 = "ColumnNumberThree";

  public final static String GROUP_BY_COLUMN = "GroupByColumn";

  public final static String TABLE_AND_COLUMN = "TableAndColumnName";

  public final static String REFINE_TYPE = "RefineType";

  public final static String PLOT_COLUMN_1 = "PlotColumnOne";
  public final static String PLOT_COLUMN_2 = "PlotColumnTwo";

  private static Set<String> getExpr = new HashSet<>();
  private static Set<String> countExpr = new HashSet<>();
  private static Set<String> averageExpr = new HashSet<>();
  private static Set<String> sumExpr = new HashSet<>();
  private static Set<String> minExpr = new HashSet<>();
  private static Set<String> maxExpr = new HashSet<>();

  private static Set<String> equalsExpr = new HashSet<>();
  private static Set<String> notEqualsExpr = new HashSet<>();
  private static Set<String> greaterExpr = new HashSet<>();
  private static Set<String> lessExpr = new HashSet<>();
  private static Set<String> greaterEqualExpr = new HashSet<>();
  private static Set<String> lessEqualExpr = new HashSet<>();

  private static Set<String> refineAddExpr = new HashSet<>();
  private static Set<String> refineReplaceExpr = new HashSet<>();
  private static Set<String> refineDropExpr = new HashSet<>();

  static {
    getExpr.addAll(Arrays.asList(new String[] {
        "get",
        "get all",
        "find",
        "find all",
        "query",
        "query for",
        "show",
        "show all",
        "visualize",
        "visualize all",
        "list",
        "list all",
        "select",
        "select all"
    }));
    countExpr.addAll(Arrays.asList(new String[] {
        "count",
        "number",
        "size",
        "length",
        "cardinality",
        "how many",
        "how much"
    }));
    averageExpr.addAll(Arrays.asList(new String[] {
        "average",
        "mean",
        "expected"
    }));
    sumExpr.addAll(Arrays.asList(new String[] {
        "sum",
        "total",
        "add up"
    }));
    minExpr.addAll(Arrays.asList(new String[] {
        "min",
        "minimum",
        "minimize",
        "least",
        "smallest"
    }));
    maxExpr.addAll(Arrays.asList(new String[] {
        "max",
        "maximum",
        "maximize",
        "most",
        "largest"
    }));
    equalsExpr.addAll(Arrays.asList(new String[]{
        "is",
        "was",
        "are",
        "were",
        "is",
        "was",
        "are",
        "were",
        "equals",
        "is equal to",
        "is same as",
        "is the same as",
        "was equal to",
        "was same as",
        "was the same as",
        "are equal to",
        "are same as",
        "are the same as",
        "were equal to",
        "were same as",
        "were the same as"
    }));
    notEqualsExpr.addAll(Arrays.asList(new String[]{
        "is not",
        "are not",
        "were not",
        "not equals",
        "is not equal to",
        "is not same as",
        "is not the same as",
        "was not equal to",
        "was not same as",
        "was not the same as",
        "are not equal to",
        "are not same as",
        "are not the same as",
        "were not equal to",
        "were not same as",
        "were not the same as"
    }));
    greaterExpr.addAll(Arrays.asList(new String[]{
        "greater",
        "above",
        "greater than",
        "more than",
        "is greater than",
        "is more than",
        "is above"
    }));
    lessExpr.addAll(Arrays.asList(new String[]{
        "less",
        "less than",
        "is less than",
        "below",
        "is below"
    }));
    greaterEqualExpr.addAll(Arrays.asList(new String[]{
        "greater or equal",
        "above or equal",
        "greater than or equal",
        "more than or equal",
        "is greater than or equal",
        "is more than or equal",
        "is above or equal"
    }));
    lessEqualExpr.addAll(Arrays.asList(new String[]{
        "less or equal",
        "less than or equal",
        "is less than or equal",
        "below or equal",
        "is below or equal"
    }));
    refineAddExpr.addAll(Arrays.asList(new String[]{
        "and",
        "and if",
        "and when",
        "and where",
        "and also",
        "and also where",
        "also",
        "also if",
        "also when",
        "also where",
        "add",
        "add where",
        "also add",
        "also add where"
    }));
    refineReplaceExpr.addAll(Arrays.asList(new String[]{
        "what if",
        "what if we",
        "what if where",
        "how about",
        "how about where",
        "how about if",
        "instead",
        "instead how about",
        "instead where",
        "instead how about where",
        "actually",
        "actually how about where"
    }));
    refineDropExpr.addAll(Arrays.asList(new String[]{
        "drop",
        "drop where",
        "drop the",
        "drop the part",
        "drop the part where",
        "drop the clause",
        "drop the clause where",
        "forget",
        "forget the",
        "forget about",
        "forget where",
        "forget about where",
        "forget the part",
        "forget the part where",
        "forget the clause",
        "forget the clause where",
        "remove",
        "remove the",
        "remove where",
        "remove the part",
        "remove the part where",
        "remove the clause",
        "remove the clause where"
    }));
  }

  public static String getFunction(String expression) {
    if (getExpr.contains(expression)) {
      return "GET";
    }
    if (countExpr.contains(expression)) {
      return "COUNT";
    }
    if (averageExpr.contains(expression)) {
      return "AVG";
    }
    if (sumExpr.contains(expression)) {
      return "SUM";
    }
    if (minExpr.contains(expression)) {
      return "MIN";
    }
    if (maxExpr.contains(expression)) {
      return "MAX";
    }
    return null;
  }

  public static String aggregationFunctionToEnglish(String aggregate) {
    switch (aggregate) {
      case "COUNT":
        return "number";
      case "AVG":
        return "average";
      case "SUM":
        return "total";
      case "MIN":
        return "minimum value";
      case "MAX":
        return "maximum value";
      default:
        return null;
    }
  }

  public static ComparisonExpression.Type getComparisonType(String expression) {
    if (equalsExpr.contains(expression)) {
      return ComparisonExpression.Type.EQUAL;
    }
    if (notEqualsExpr.contains(expression)) {
      return ComparisonExpression.Type.NOT_EQUAL;
    }
    if (greaterExpr.contains(expression)) {
      return ComparisonExpression.Type.GREATER_THAN;
    }
    if (lessExpr.contains(expression)) {
      return ComparisonExpression.Type.LESS_THAN;
    }
    if (greaterEqualExpr.contains(expression)) {
      return ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
    }
    if (lessEqualExpr.contains(expression)) {
      return ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
    }
    return null;
  }

  public static String comparisonTypeToEnglish(ComparisonExpression.Type type) {
    switch (type) {
      case EQUAL:
        return " equal to ";
      case NOT_EQUAL:
        return " not equal to ";
      case LESS_THAN:
        return " less than ";
      case GREATER_THAN:
        return " greater than ";
      case GREATER_THAN_OR_EQUAL:
        return " greater than or equal to ";
      case IS_DISTINCT_FROM:
        return " distinct from ";
      default:
        return null;
    }
  }

  public static LogicalBinaryExpression.Type getComparisonBinaryOperatorType(
      String op) {
    if (op.equals("and")) {
      return LogicalBinaryExpression.Type.AND;
    }
    if (op.equals("or")) {
      return LogicalBinaryExpression.Type.OR;
    }
    return null;
  }

  public static ColumnName parseColumnSlot(@Nullable String columnName) {
    // WARNING: Here, there be dataset specific hacks!
    if (columnName == null) {
      return new ColumnName();
    }
    String[] parts = columnName.split(" ");
    if (parts.length == 1) {
      return new ColumnName(null, parts[0], ColumnType.UNKNOWN);
    } else {
      return new ColumnName(
          parts[0].endsWith("s") ? parts[0] : (parts[0] + "s"),
          parts[1],
          ColumnType.UNKNOWN);
    }
  }

  public static RefineType getRefineType(@Nullable String expression) {
    if (expression == null) {
      return RefineType.REPLACE;
    }
    if (refineAddExpr.contains(expression)) {
      return RefineType.ADD;
    }
    if (refineReplaceExpr.contains(expression)) {
      return RefineType.REPLACE;
    }
    if (refineDropExpr.contains(expression)) {
      return RefineType.DROP;
    }
    return null;
  }

  private SlotUtil() {}
}
