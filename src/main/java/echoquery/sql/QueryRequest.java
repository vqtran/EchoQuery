package echoquery.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils;

import com.amazon.speech.slu.Intent;
import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.StringLiteral;

import echoquery.sql.joins.JoinRecipe;
import echoquery.utils.SlotUtil;

public class QueryRequest {
  public enum ComparisonValueType {
    STRING,
    NUMBER,
  }

  // Metadata about the original request parsed from the intent.
  private String fromTable;
  private String aggregationFunc;
  private String aggregationColumn;
  private List<String> comparisonColumns;
  private List<ComparisonExpression.Type> comparators;
  private List<String> comparisonValues;
  private List<ComparisonValueType> comparisonValueType;
  private List<LogicalBinaryExpression.Type> comparisonBinaryOperators;

  // The built query object itself.
  private Query query;

  public QueryRequest() {
    comparisonColumns = new ArrayList<>();
    comparators = new ArrayList<>();
    comparisonValues = new ArrayList<>();
    comparisonValueType = new ArrayList<>();
    comparisonBinaryOperators = new ArrayList<>();
  }

  public QueryRequest setFromTable(String table) {
    fromTable = table;
    return this;
  }

  public QueryRequest setAggregationFunc(String func) {
    aggregationFunc = SlotUtil.getAggregateFunction(func);
    return this;
  }

  public QueryRequest setAggregationColumn(String col) {
    aggregationColumn = col;
    return this;
  }

  public QueryRequest addWhereClause(
      String comparisonColumn,
      String comparator,
      String comparisonValue,
      ComparisonValueType type) {
    if (comparisonColumn != null) {
      comparisonColumns.add(comparisonColumn);
      comparators.add(SlotUtil.getComparisonType(comparator));
      comparisonValues.add(comparisonValue);
      comparisonValueType.add(type);
    }
    return this;
  }

  public QueryRequest addWhereClause(
      String comparisonBinaryOperator,
      String comparisonColumn,
      String comparator,
      String comparisonValue,
      ComparisonValueType type) {
    if (comparisonBinaryOperator != null) {
      comparisonBinaryOperators.add(
          SlotUtil.getComparisonBinaryOperatorType(comparisonBinaryOperator));
      addWhereClause(comparisonColumn, comparator, comparisonValue, type);
    }
    return this;
  }

  public QueryRequest buildQuery() {
    SchemaInferrer inferrer = SchemaInferrer.getInstance();
    JoinRecipe from = inferrer.infer(
        fromTable, aggregationColumn, comparisonColumns);

    Expression aggregationExp;
    if (aggregationColumn == null) {
      aggregationExp = QueryUtil.functionCall(aggregationFunc);
    } else {
      aggregationExp = QueryUtil.functionCall(
          aggregationFunc,
          new QualifiedNameReference(QualifiedName.of(
              from.getAggregationPrefix(), aggregationColumn)));
    }

    List<Expression> whereClauses = new ArrayList<>();
    for (int i = 0; i < comparisonColumns.size(); i++) {
      whereClauses.add(new ComparisonExpression(
          comparators.get(i),
          new QualifiedNameReference(QualifiedName.of(
              from.getComparisonPrefix(i), comparisonColumns.get(i))),
          (comparisonValueType.get(i) == ComparisonValueType.NUMBER)
            ? new LongLiteral(comparisonValues.get(i))
            : new StringLiteral(comparisonValues.get(i))));
    }

    if (whereClauses.isEmpty()) {
      query = QueryUtil.simpleQuery(
          QueryUtil.selectList(aggregationExp), from.render());
    } else {
      query = QueryUtil.simpleQuery(
          QueryUtil.selectList(aggregationExp),
          from.render(),
          logicallyCombineWhereClauses(
              whereClauses, comparisonBinaryOperators, 0));
    }

    return this;
  }

  private static Expression logicallyCombineWhereClauses(
      List<Expression> whereClauses,
      List<LogicalBinaryExpression.Type> binaryOps,
      int i) {
    if (i >= binaryOps.size()) {
      return whereClauses.get(i);
    }
    return new LogicalBinaryExpression(
        binaryOps.get(i),
        whereClauses.get(i),
        logicallyCombineWhereClauses(whereClauses, binaryOps, i + 1));
  }

  public Query getQuery() {
    return query;
  }

  public String getFromTable() {
    return fromTable;
  }

  public String getAggregationFunc() {
    return aggregationFunc;
  }

  public static QueryRequest of(Intent intent) {
    return new QueryRequest()
        .setFromTable(intent.getSlot(SlotUtil.TABLE_NAME).getValue())
        .setAggregationFunc(intent.getSlot(SlotUtil.AGGREGATE).getValue())
        .setAggregationColumn(
            intent.getSlot(SlotUtil.AGGREGATION_COLUMN).getValue())
        .addWhereClause(
            intent.getSlot(SlotUtil.COMPARISON_COLUMN_1).getValue(),
            intent.getSlot(SlotUtil.COMPARATOR_1).getValue(),
            ObjectUtils.defaultIfNull(
                intent.getSlot(SlotUtil.COLUMN_VALUE_1).getValue(),
                intent.getSlot(SlotUtil.COLUMN_NUMBER_1).getValue()),
            (intent.getSlot(SlotUtil.COLUMN_NUMBER_1).getValue() != null)
              ? ComparisonValueType.NUMBER : ComparisonValueType.STRING)
        .addWhereClause(
            intent.getSlot(SlotUtil.BINARY_LOGIC_OP_1).getValue(),
            intent.getSlot(SlotUtil.COMPARISON_COLUMN_2).getValue(),
            intent.getSlot(SlotUtil.COMPARATOR_2).getValue(),
            ObjectUtils.defaultIfNull(
                intent.getSlot(SlotUtil.COLUMN_VALUE_2).getValue(),
                intent.getSlot(SlotUtil.COLUMN_NUMBER_2).getValue()),
            (intent.getSlot(SlotUtil.COLUMN_NUMBER_2).getValue() != null)
              ? ComparisonValueType.NUMBER : ComparisonValueType.STRING)
        .addWhereClause(
            intent.getSlot(SlotUtil.BINARY_LOGIC_OP_2).getValue(),
            intent.getSlot(SlotUtil.COMPARISON_COLUMN_3).getValue(),
            intent.getSlot(SlotUtil.COMPARATOR_3).getValue(),
            ObjectUtils.defaultIfNull(
                intent.getSlot(SlotUtil.COLUMN_VALUE_3).getValue(),
                intent.getSlot(SlotUtil.COLUMN_NUMBER_3).getValue()),
            (intent.getSlot(SlotUtil.COLUMN_NUMBER_3).getValue() != null)
              ? ComparisonValueType.NUMBER : ComparisonValueType.STRING)
        .buildQuery();
  }
}
