package echoquery.querier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang3.ObjectUtils;

import com.amazon.speech.slu.Intent;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import echoquery.querier.schema.ColumnName;
import echoquery.querier.schema.ColumnType;
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
  private Optional<String> selectAll;
  private Optional<String> aggregationFunc;
  private ColumnName aggregationColumn;
  private List<ColumnName> comparisonColumns;
  private List<Optional<ComparisonExpression.Type>> comparators;
  private List<Optional<String>> comparisonValues;
  private List<Optional<LogicalBinaryExpression.Type>>
      comparisonBinaryOperators;
  private ColumnName groupByColumn;

  public QueryRequest() {
    fromTable = Optional.absent();
    selectAll = Optional.absent();
    aggregationFunc = Optional.absent();
    aggregationColumn = new ColumnName();
    comparisonColumns = new ArrayList<>();
    comparators = new ArrayList<>();
    comparisonValues = new ArrayList<>();
    comparisonBinaryOperators = new ArrayList<>();
    groupByColumn = new ColumnName();
  }

  /**
   * Construct a QueryRequest from an intent. Accesses all the relevant slots
   * from the intent and uses all relevant methods of QueryRequest to populate
   * QueryRequest.
   * @param intent
   * @return a new populated QueryRequest.
   */
  public static QueryRequest of(Intent intent) {
    QueryRequest request = new QueryRequest()
        // FROM
        .setFromTable(intent.getSlot(SlotUtil.TABLE_NAME).getValue())
        // AGGREGATION FUNCTION
        .setFunc(intent.getSlot(SlotUtil.FUNC).getValue())
        // AGGREGATION COLUMN
        .setAggregationColumn(SlotUtil.parseColumnSlot(
            intent.getSlot(SlotUtil.AGGREGATION_COLUMN).getValue()))
        // WHERE CLAUSE 1
        .addWhereClause(SlotUtil.parseColumnSlot(
            // WHERE CLAUSE 1 COLUMN
            intent.getSlot(SlotUtil.COMPARISON_COLUMN_1).getValue())
                .setType((intent.getSlot(
                    SlotUtil.COLUMN_NUMBER_1).getValue() != null)
                ? ColumnType.NUMBER : ColumnType.STRING),
            // WHERE CLAUSE 1 COMPARATOR
            intent.getSlot(SlotUtil.COMPARATOR_1).getValue(),
            // WHERE CLAUSE 1 VALUE
            ObjectUtils.defaultIfNull(
                intent.getSlot(SlotUtil.COLUMN_VALUE_1).getValue(),
                intent.getSlot(SlotUtil.COLUMN_NUMBER_1).getValue()))
        // WHERE CLAUSE 2
        .addWhereClause(
            // OPERATOR BETWEEN WHERE 1 and WHERE 2.
            intent.getSlot(SlotUtil.BINARY_LOGIC_OP_1).getValue(),
            // WHERE CLAUSE 2 COLUMN
            SlotUtil.parseColumnSlot(
                intent.getSlot(SlotUtil.COMPARISON_COLUMN_2).getValue())
                    .setType((intent.getSlot(
                        SlotUtil.COLUMN_NUMBER_2).getValue() != null)
                ? ColumnType.NUMBER : ColumnType.STRING),
            // WHERE CLAUSE 2 COMPARATOR
            intent.getSlot(SlotUtil.COMPARATOR_2).getValue(),
            // WHERE CLAUSE 2 VALUE
            ObjectUtils.defaultIfNull(
                intent.getSlot(SlotUtil.COLUMN_VALUE_2).getValue(),
                intent.getSlot(SlotUtil.COLUMN_NUMBER_2).getValue()))
        // WHERE CLAUSE 3
        .addWhereClause(
            // OPERATOR BETWEEN WHERE 2 and WHERE 3.
            intent.getSlot(SlotUtil.BINARY_LOGIC_OP_2).getValue(),
            // WHERE CLAUSE 3 COLUMN
            SlotUtil.parseColumnSlot(
                intent.getSlot(SlotUtil.COMPARISON_COLUMN_3).getValue())
                    .setType((intent.getSlot(
                        SlotUtil.COLUMN_NUMBER_3).getValue() != null)
                ? ColumnType.NUMBER : ColumnType.STRING),
            // WHERE CLAUSE 3 COMPARATOR
            intent.getSlot(SlotUtil.COMPARATOR_3).getValue(),
            // WHERE CLAUSE 3 VALUE
            ObjectUtils.defaultIfNull(
                intent.getSlot(SlotUtil.COLUMN_VALUE_3).getValue(),
                intent.getSlot(SlotUtil.COLUMN_NUMBER_3).getValue()))
        // GROUP-BY (COLUMN TO GROUP BY)
        .setGroupByColumn(SlotUtil.parseColumnSlot(
            intent.getSlot(SlotUtil.GROUP_BY_COLUMN).getValue()));
    return request;
  }

  public Optional<String> getFromTable() {
    return fromTable;
  }

  public Optional<String> getSelectAll() {
    return selectAll;
  }

  public Optional<String> getAggregationFunc() {
    return aggregationFunc;
  }

  public ColumnName getAggregationColumn() {
    return aggregationColumn;
  }

  public List<ColumnName> getComparisonColumns() {
    return ImmutableList.copyOf(comparisonColumns);
  }

  public List<Optional<ComparisonExpression.Type>> getComparators() {
    return ImmutableList.copyOf(comparators);
  }

  public List<Optional<String>> getComparisonValues() {
    return ImmutableList.copyOf(comparisonValues);
  }

  public List<Optional<LogicalBinaryExpression.Type>>
      getComparisonBinaryOperators() {
    return ImmutableList.copyOf(comparisonBinaryOperators);
  }

  public ColumnName getGroupByColumn() {
    return groupByColumn;
  }

  public QueryRequest setFromTable(@Nullable String table) {
    fromTable = Optional.fromNullable(table);
    return this;
  }

  public QueryRequest setFunc(@Nullable String func) {
    Optional<String> f = Optional.fromNullable(SlotUtil.getFunction(func));

    // Sets selectAll by default.
    if (!f.isPresent()) {
      selectAll = Optional.of(SlotUtil.getFunction("get"));
    } else if (f.get().equals("GET")) {
      selectAll = f;
    } else {
      aggregationFunc = f;
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
      return this;
    } else {
      return addWhereClause(comparisonColumn, comparator, comparisonValue);
    }
  }

  /**
   * Modify an existing where clause referenced by an existing column name with
   * new comparator and new comparison value.
   * @param existingColumn
   * @param newComparator
   * @param newComparisonValue
   * @return
   */
  public QueryRequest modifyWhereClause(
      ColumnName existingColumn,
      @Nullable String newComparator,
      @Nullable String newComparisonValue) {
    for (int i = 0; i < comparisonColumns.size(); i++) {
      if (existingColumn.equals(comparisonColumns.get(i))) {
        comparators.set(i,
            Optional.fromNullable(SlotUtil.getComparisonType(newComparator)));
        comparisonValues.set(i, Optional.fromNullable(newComparisonValue));
        break;
      }
    }
    return this;
  }

  /**
   * Remove an existing where clause referenced by an existing column name.
   * @param existingColumn
   * @return
   */
  public QueryRequest removeWhereClause(ColumnName existingColumn) {
    for (int i = 0; i < comparisonColumns.size(); i++) {
      if (existingColumn.equals(comparisonColumns.get(i))) {
        comparisonColumns.remove(i);
        comparators.remove(i);
        comparisonValues.remove(i);
        if (!comparisonBinaryOperators.isEmpty()) {
          comparisonBinaryOperators.remove(Math.max(0, i-1));
        }
        break;
      }
    }
    return this;
  }

  /**
   * Removes all where clauses - all associated columns, values, and operators.
   */
  public QueryRequest removeAllWhereClauses() {
    comparisonBinaryOperators.clear();
    comparisonColumns.clear();
    comparators.clear();
    comparisonValues.clear();
    return this;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((aggregationColumn == null)
        ? 0 : aggregationColumn.hashCode());
    result = prime * result + ((selectAll == null)
        ? 0 : selectAll.hashCode());
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
    if (selectAll == null) {
      if (other.getSelectAll() != null)
        return false;
    } else if (!selectAll.equals(other.getSelectAll()))
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
    return true;
  }
}
