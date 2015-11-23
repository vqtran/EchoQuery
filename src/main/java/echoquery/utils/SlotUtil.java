package echoquery.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;

public final class SlotUtil {
  public final static String TABLE_NAME = "TableName";

  public final static String AGGREGATE = "Aggregate";
  public final static String AGGREGATION_COLUMN = "AggregationColumn";

  public final static String COMPARISON_COLUMN_1 = "ComparisonColumn1";
  public final static String COMPARATOR_1 = "Comparator1";
  public final static String COLUMN_VALUE_1 = "ColumnValue1";
  public final static String COLUMN_NUMBER_1 = "ColumnNumber1";

  public final static String BINARY_LOGIC_OP_1 = "BinaryLogicOp1";

  public final static String COMPARISON_COLUMN_2 = "ComparisonColumn2";
  public final static String COMPARATOR_2 = "Comparator2";
  public final static String COLUMN_VALUE_2 = "ColumnValue2";
  public final static String COLUMN_NUMBER_2 = "ColumnNumber2";

  public final static String BINARY_LOGIC_OP_2 = "BinaryLogicOp2";

  public final static String COMPARISON_COLUMN_3 = "ComparisonColumn3";
  public final static String COMPARATOR_3 = "Comparator3";
  public final static String COLUMN_VALUE_3 = "ColumnValue3";
  public final static String COLUMN_NUMBER_3 = "ColumnNumber3";

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

  static {
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
  }

  public static String getAggregateFunction(String expression) {
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

  private SlotUtil() {}
}
