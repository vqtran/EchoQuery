package echoquery.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class SlotUtil {
  public final static String AGGREGATE = "Aggregate";
  public final static String TABLE_NAME = "TableName";
  public final static String COLUMN_NAME = "ColumnName";
  public final static String COLUMN_VALUE = "ColumnValue";
  public final static String COLUMN_NUMBER = "ColumnNumber";
  public final static String COMPARATOR = "Comparator";

  private static Set<String> countExpr = new HashSet<>();
  private static Set<String> averageExpr = new HashSet<>();
  private static Set<String> sumExpr = new HashSet<>();
  private static Set<String> minExpr = new HashSet<>();
  private static Set<String> maxExpr = new HashSet<>();

  private static Set<String> equalsExpr = new HashSet<>();
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

  private SlotUtil() {}

}
