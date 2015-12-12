package echoquery.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang3.ObjectUtils;

import com.amazon.speech.slu.Intent;
import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import echoquery.sql.joins.InferredContext;
import echoquery.sql.joins.JoinRecipe;
import echoquery.sql.model.ColumnName;
import echoquery.sql.model.ColumnType;
import echoquery.utils.SlotUtil;

/**
 * A QueryRequests holds all of the information necessary to build a query AST
 * object, and hold original request fields.
 */

public class QueryRequest implements Serializable {
  /**
   * Generated for serialization.
   */
  private static final long serialVersionUID = 5356143401459952793L;

  /**
   * Metadata about the original request parsed from the intent. Assumption
   * made that there will always only be one aggregation. Index i in lists
   * for comparisons correspond to one where. Everything could be Optional.empty
   * to support malformed requests.
   */
  private Optional<String> fromTable;
  private Optional<String> aggregationFunc;
  private ColumnName aggregationColumn;
  private List<ColumnName> comparisonColumns;
  private List<Optional<ComparisonExpression.Type>> comparators;
  private List<Optional<String>> comparisonValues;
  private List<Optional<LogicalBinaryExpression.Type>>
      comparisonBinaryOperators;
  private ColumnName groupByColumn;

  // The built Query AST object itself.
  private Query query;
  // And what was inferred about the column.
  private InferredContext ctx;

  public QueryRequest() {
    fromTable = Optional.absent();
    aggregationFunc = Optional.absent();
    aggregationColumn = new ColumnName();
    comparisonColumns = new ArrayList<>();
    comparators = new ArrayList<>();
    comparisonValues = new ArrayList<>();
    comparisonBinaryOperators = new ArrayList<>();
    groupByColumn = new ColumnName();
  }

  public Optional<String> getFromTable() {
    return fromTable;
  }

  public Optional<String> getAggregationFunc() {
    return aggregationFunc;
  }

  public ColumnName getAggregationColumn() {
    return aggregationColumn;
  }

  public List<ColumnName> getComparisonColumns() {
    return comparisonColumns;
  }

  public List<Optional<ComparisonExpression.Type>> getComparators() {
    return comparators;
  }

  public List<Optional<String>> getComparisonValues() {
    return comparisonValues;
  }

  public List<Optional<LogicalBinaryExpression.Type>>
      getComparisonBinaryOperators() {
    return comparisonBinaryOperators;
  }

  public ColumnName getGroupByColumn() {
    return groupByColumn;
  }

  public Query getQuery() {
    return query;
  }

  public InferredContext getContext() {
    return ctx;
  }

  public QueryRequest setFromTable(@Nullable String table) {
    fromTable = Optional.fromNullable(table);
    return this;
  }

  public QueryRequest setAggregationFunc(@Nullable String func) {
    aggregationFunc =
        Optional.fromNullable(SlotUtil.getAggregateFunction(func));

    // Sets the aggregation function to count by default.
    if (!aggregationFunc.isPresent()) {
      aggregationFunc = Optional.of(SlotUtil.getAggregateFunction("count"));
    }
    return this;
  }

  public QueryRequest setAggregationColumn(ColumnName column) {
    aggregationColumn = column;
    return this;
  }

  public QueryRequest setGroupByColumn(ColumnName column) {
    groupByColumn = column;
    return this;
  }

  /**
   * Adds each component of the where clause to their respective Lists.
   * @param comparisonColumn
   * @param comparator
   * @param comparisonValue
   * @return
   */
  public QueryRequest addWhereClause(
      ColumnName comparisonColumn,
      @Nullable String comparator,
      @Nullable String comparisonValue) {
    if (comparisonColumn.getColumn().isPresent()
        || comparator != null
        || comparisonValue != null) {
      comparisonColumns.add(comparisonColumn);
      comparators.add(
          Optional.fromNullable(SlotUtil.getComparisonType(comparator)));
      comparisonValues.add(Optional.fromNullable(comparisonValue));
    }
    return this;
  }

  /**
   * Adds each component of the where clause to their respective Lists. This
   * variant used for non-first where clauses to specify AND or OR.
   * @param comparisonBinaryOperator
   * @param comparisonColumn
   * @param comparator
   * @param comparisonValue
   * @return
   */
  public QueryRequest addWhereClause(
      @Nullable String comparisonBinaryOperator,
      ColumnName comparisonColumn,
      @Nullable String comparator,
      @Nullable String comparisonValue) {
    if (comparisonBinaryOperator != null) {
      comparisonBinaryOperators.add(Optional.fromNullable(
          SlotUtil.getComparisonBinaryOperatorType(comparisonBinaryOperator)));
      comparisonColumns.add(comparisonColumn);
      comparators.add(
          Optional.fromNullable(SlotUtil.getComparisonType(comparator)));
      comparisonValues.add(Optional.fromNullable(comparisonValue));
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
  public QueryRequest buildQuery(SchemaInferrer inferrer)
      throws QueryBuildException {
    JoinRecipe from = inferrer.infer(
        fromTable.get(), aggregationColumn, comparisonColumns, groupByColumn);
    ctx = from.getContext();

    if (!from.isValid()) {
      throw new QueryBuildException(QueryResult.of(this, from));
    }

    Expression aggregationExp;
    if (aggregationColumn.getColumn().isPresent()) {
      aggregationExp = QueryUtil.functionCall(
          aggregationFunc.get(),
          new QualifiedNameReference(QualifiedName.of(
              ctx.getAggregationPrefix().get(),
              aggregationColumn.getColumn().get())));
    } else {
      aggregationExp = QueryUtil.functionCall(aggregationFunc.get());
    }

    List<Expression> whereClauses = new ArrayList<>();
    for (int i = 0; i < comparisonColumns.size(); i++) {
      ColumnName col = comparisonColumns.get(i);
      whereClauses.add(new ComparisonExpression(
          comparators.get(i).get(),
          new QualifiedNameReference(QualifiedName.of(
              ctx.getComparisonPrefix(i).get(), col.getColumn().get())),
          (col.getType() == ColumnType.NUMBER)
            ? new LongLiteral(comparisonValues.get(i).get())
            : new StringLiteral(comparisonValues.get(i).get())));
    }

    QualifiedNameReference groupByName = null;
    List<GroupingElement> groupBy = new ArrayList<>();
    if (groupByColumn.getColumn().isPresent()) {
      groupByName = new QualifiedNameReference(QualifiedName.of(
          ctx.getGroupByPrefix().get(), groupByColumn.getColumn().get()));
      groupBy.add(new SimpleGroupBy(ImmutableList.of(groupByName)));
    }

    query = QueryUtil.simpleQuery(
        (groupByColumn.getColumn().isPresent())
            ? QueryUtil.selectList(groupByName, aggregationExp)
            : QueryUtil.selectList(aggregationExp),
        from.render(),
        java.util.Optional.ofNullable(logicallyCombineWhereClauses(
            whereClauses, comparisonBinaryOperators, 0)),
        groupBy,
        java.util.Optional.empty(),
        ImmutableList.of(),
        java.util.Optional.empty());

    return this;
  }

  /**
   * Assembles the ComparisonExpressions into a single Expression combined
   * using the specified Binary Operators, in order.
   * @param whereClauses
   * @param binaryOps
   * @param i
   * @return
   */
  private static Expression logicallyCombineWhereClauses(
      List<Expression> whereClauses,
      List<Optional<LogicalBinaryExpression.Type>> binaryOps,
      int i) {
    if (whereClauses.isEmpty()) {
      return null;
    }
    if (i >= binaryOps.size()) {
      return whereClauses.get(i);
    }
    return new LogicalBinaryExpression(
        binaryOps.get(i).get(),
        whereClauses.get(i),
        logicallyCombineWhereClauses(whereClauses, binaryOps, i + 1));
  }

  /**
   * Construct a QueryRequest from an intent. The Query AST field here part will
   * NOT be built yet. buildQuery should only be called after validation.
   * @param intent
   * @return
   */
  public static QueryRequest of(Intent intent) {
    QueryRequest request = new QueryRequest()
        .setFromTable(intent.getSlot(SlotUtil.TABLE_NAME).getValue())
        .setAggregationFunc(intent.getSlot(SlotUtil.AGGREGATE).getValue())
        .setAggregationColumn(SlotUtil.parseColumnSlot(
            intent.getSlot(SlotUtil.AGGREGATION_COLUMN).getValue()))
        .addWhereClause(SlotUtil.parseColumnSlot(
            intent.getSlot(SlotUtil.COMPARISON_COLUMN_1).getValue())
                .setType((intent.getSlot(
                    SlotUtil.COLUMN_NUMBER_1).getValue() != null)
                ? ColumnType.NUMBER : ColumnType.STRING),
            intent.getSlot(SlotUtil.COMPARATOR_1).getValue(),
            ObjectUtils.defaultIfNull(
                intent.getSlot(SlotUtil.COLUMN_VALUE_1).getValue(),
                intent.getSlot(SlotUtil.COLUMN_NUMBER_1).getValue()))
        .addWhereClause(
            intent.getSlot(SlotUtil.BINARY_LOGIC_OP_1).getValue(),
            SlotUtil.parseColumnSlot(
                intent.getSlot(SlotUtil.COMPARISON_COLUMN_2).getValue())
                    .setType((intent.getSlot(
                        SlotUtil.COLUMN_NUMBER_2).getValue() != null)
                ? ColumnType.NUMBER : ColumnType.STRING),
            intent.getSlot(SlotUtil.COMPARATOR_2).getValue(),
            ObjectUtils.defaultIfNull(
                intent.getSlot(SlotUtil.COLUMN_VALUE_2).getValue(),
                intent.getSlot(SlotUtil.COLUMN_NUMBER_2).getValue()))
        .addWhereClause(
            intent.getSlot(SlotUtil.BINARY_LOGIC_OP_2).getValue(),
            SlotUtil.parseColumnSlot(
                intent.getSlot(SlotUtil.COMPARISON_COLUMN_3).getValue())
                    .setType((intent.getSlot(
                        SlotUtil.COLUMN_NUMBER_3).getValue() != null)
                ? ColumnType.NUMBER : ColumnType.STRING),
            intent.getSlot(SlotUtil.COMPARATOR_3).getValue(),
            ObjectUtils.defaultIfNull(
                intent.getSlot(SlotUtil.COLUMN_VALUE_3).getValue(),
                intent.getSlot(SlotUtil.COLUMN_NUMBER_3).getValue()))
        .setGroupByColumn(SlotUtil.parseColumnSlot(
            intent.getSlot(SlotUtil.GROUP_BY_COLUMN).getValue()));
    return request;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((aggregationColumn == null)
        ? 0 : aggregationColumn.hashCode());
    result = prime * result + ((aggregationFunc == null)
        ? 0 : aggregationFunc.hashCode());
    result = prime * result + ((comparators == null)
        ? 0 : comparators.hashCode());
    result = prime * result + ((comparisonBinaryOperators == null)
        ? 0 : comparisonBinaryOperators.hashCode());
    result = prime * result + ((comparisonColumns == null)
        ? 0 : comparisonColumns.hashCode());
    result = prime * result + ((comparisonValues == null)
        ? 0 : comparisonValues.hashCode());
    result = prime * result + ((fromTable == null)
        ? 0 : fromTable.hashCode());
    result = prime * result + ((groupByColumn == null)
        ? 0 : groupByColumn.hashCode());
    result = prime * result + ((query == null) ? 0 : query.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    QueryRequest other = (QueryRequest) obj;
    if (aggregationColumn == null) {
      if (other.getAggregationColumn() != null)
        return false;
    } else if (!aggregationColumn.equals(other.getAggregationColumn()))
      return false;
    if (aggregationFunc == null) {
      if (other.getAggregationFunc() != null)
        return false;
    } else if (!aggregationFunc.equals(other.getAggregationFunc()))
      return false;
    if (comparators == null) {
      if (other.getComparators() != null)
        return false;
    } else if (!comparators.equals(other.getComparators()))
      return false;
    if (comparisonBinaryOperators == null) {
      if (other.getComparisonBinaryOperators() != null)
        return false;
    } else if (
        !comparisonBinaryOperators.equals(other.getComparisonBinaryOperators()))
      return false;
    if (comparisonColumns == null) {
      if (other.getComparisonColumns() != null)
        return false;
    } else if (!comparisonColumns.equals(other.getComparisonColumns()))
      return false;
    if (comparisonValues == null) {
      if (other.getComparisonValues() != null)
        return false;
    } else if (!comparisonValues.equals(other.getComparisonValues()))
      return false;
    if (fromTable == null) {
      if (other.getFromTable() != null)
        return false;
    } else if (!fromTable.equals(other.getFromTable()))
      return false;
    if (groupByColumn == null) {
      if (other.getGroupByColumn() != null)
        return false;
    } else if (!groupByColumn.equals(other.getGroupByColumn()))
      return false;
    if (query == null) {
      if (other.getQuery() != null)
        return false;
    } else if (!query.equals(other.getQuery()))
      return false;
    return true;
  }
}
