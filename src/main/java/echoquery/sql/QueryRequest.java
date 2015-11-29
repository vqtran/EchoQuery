package echoquery.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

  /**
   * Metadata about the original request parsed from the intent. Assumption
   * made that there will always only be one aggregation. Index i in lists
   * for comparisons correspond to one where. Everything could be Optional.empty
   * to support malformed requests.
   */
  private Optional<String> fromTable;
  private Optional<String> aggregationFunc;
  private Optional<String> aggregationColumn;
  private List<Optional<String>> comparisonColumns;
  private List<Optional<ComparisonExpression.Type>> comparators;
  private List<Optional<String>> comparisonValues;
  private List<Optional<ComparisonValueType>> comparisonValueType;
  private List<Optional<LogicalBinaryExpression.Type>>
      comparisonBinaryOperators;

  // The built query object itself.
  private Query query;

  public QueryRequest() {
    fromTable = Optional.empty();
    aggregationFunc = Optional.empty();
    aggregationColumn = Optional.empty();
    comparisonColumns = new ArrayList<>();
    comparators = new ArrayList<>();
    comparisonValues = new ArrayList<>();
    comparisonValueType = new ArrayList<>();
    comparisonBinaryOperators = new ArrayList<>();
  }

  public Optional<String> getFromTable() {
    return fromTable;
  }

  public Optional<String> getAggregationFunc() {
    return aggregationFunc;
  }

  public Optional<String> getAggregationColumn() {
    return aggregationColumn;
  }

  public List<Optional<String>> getComparisonColumns() {
    return comparisonColumns;
  }

  public List<Optional<ComparisonExpression.Type>> getComparators() {
    return comparators;
  }

  public List<Optional<String>> getComparisonValues() {
    return comparisonValues;
  }

  public List<Optional<ComparisonValueType>> getComparisonValueTypes() {
    return comparisonValueType;
  }

  public List<Optional<LogicalBinaryExpression.Type>>
      getComparisonBinaryOperators() {
    return comparisonBinaryOperators;
  }

  public Query getQuery() {
    return query;
  }

  public QueryRequest setFromTable(String table) {
    fromTable = Optional.ofNullable(table);
    return this;
  }

  public QueryRequest setAggregationFunc(String func) {
    aggregationFunc = Optional.ofNullable(SlotUtil.getAggregateFunction(func));
    return this;
  }

  public QueryRequest setAggregationColumn(String col) {
    aggregationColumn = Optional.ofNullable(col);
    return this;
  }

  public QueryRequest addWhereClause(
      String comparisonColumn,
      String comparator,
      String comparisonValue,
      ComparisonValueType type) {
    if (comparisonColumn != null
        || comparator != null
        || comparisonValue != null) {
      comparisonColumns.add(Optional.ofNullable(comparisonColumn));
      comparators.add(
          Optional.ofNullable(SlotUtil.getComparisonType(comparator)));
      comparisonValues.add(Optional.ofNullable(comparisonValue));
      comparisonValueType.add(Optional.ofNullable(type));
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
      comparisonBinaryOperators.add(Optional.ofNullable(
          SlotUtil.getComparisonBinaryOperatorType(comparisonBinaryOperator)));
      comparisonColumns.add(Optional.ofNullable(comparisonColumn));
      comparators.add(
          Optional.ofNullable(SlotUtil.getComparisonType(comparator)));
      comparisonValues.add(Optional.ofNullable(comparisonValue));
      comparisonValueType.add(Optional.ofNullable(type));
    }
    return this;
  }

  /**
   * Builds the AST query object with the current request metadata, and schema
   * inference engine. Assumes that the metadata has been validated, and so
   * necessary names are non-empty, and lists are properly structured.
   *
   * @param inferrer
   * @return this query request with query built.
   */
  public QueryRequest buildQuery(SchemaInferrer inferrer) {
    List<String> validComparisonColumns = new ArrayList<>();
    for (Optional<String> c : comparisonColumns) {
      validComparisonColumns.add(c.get());
    }

    JoinRecipe from = inferrer.infer(fromTable.get(),
        aggregationColumn.orElseGet(() -> null), validComparisonColumns);

    Expression aggregationExp;
    if (aggregationColumn.isPresent()) {
      aggregationExp = QueryUtil.functionCall(
          aggregationFunc.get(),
          new QualifiedNameReference(QualifiedName.of(
              from.getAggregationPrefix(), aggregationColumn.get())));
    } else {
      aggregationExp = QueryUtil.functionCall(aggregationFunc.get());
    }

    List<Expression> whereClauses = new ArrayList<>();
    for (int i = 0; i < comparisonColumns.size(); i++) {
      whereClauses.add(new ComparisonExpression(
          comparators.get(i).get(),
          new QualifiedNameReference(QualifiedName.of(
              from.getComparisonPrefix(i), comparisonColumns.get(i).get())),
          (comparisonValueType.get(i).get() == ComparisonValueType.NUMBER)
            ? new LongLiteral(comparisonValues.get(i).get())
            : new StringLiteral(comparisonValues.get(i).get())));
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
      List<Optional<LogicalBinaryExpression.Type>> binaryOps,
      int i) {
    if (i >= binaryOps.size()) {
      return whereClauses.get(i);
    }
    return new LogicalBinaryExpression(
        binaryOps.get(i).get(),
        whereClauses.get(i),
        logicallyCombineWhereClauses(whereClauses, binaryOps, i + 1));
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
              ? ComparisonValueType.NUMBER : ComparisonValueType.STRING);
  }
}
