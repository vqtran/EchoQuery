package echoquery.querier.translate;

import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpression.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import echoquery.querier.QueryBuildException;
import echoquery.querier.QueryRequest;
import echoquery.querier.QueryResult;
import echoquery.querier.infer.InferredContext;
import echoquery.querier.infer.JoinRecipe;
import echoquery.querier.infer.SchemaInferrer;
import echoquery.querier.schema.ColumnName;
import echoquery.querier.schema.ColumnType;
import echoquery.querier.translate.sql.SqlFormatter;

/**
 * Translates a valid QueryRequest into a SQL query string, inserting any
 * necessary inferred joins. Uses the Presto AST for the translation.
 */

public class RequestTranslator {

  private SchemaInferrer inferrer;

  public RequestTranslator(SchemaInferrer inferrer) {
    this.inferrer = inferrer;
  }

  /**
   * Translate a validated QueryRequest into a SQL query string.
   * @throws QueryBuildException if no valid joins could be found.
   */
  public String translate(QueryRequest request) throws QueryBuildException {
    return SqlFormatter.formatSql(buildAST(request));
  }

  /**
   * Builds the AST query object with the current QueryRequest and schema
   * inference engine. Assumes that QueryRequest has been validated, and so
   * necessary names are non-empty, and lists are properly structured.
   *
   * @param request Properly validated.
   * @return a SQL AST compiled from the QueryRequest.
   */
  public Query buildAST(QueryRequest request) throws QueryBuildException {
    // INFER JOINS. Figure out the Relation to select from.
    JoinRecipe from = inferrer.infer(
        request.getFromTable().get(),
        request.getAggregationColumn(),
        request.getComparisonColumns(),
        request.getGroupByColumn());
    InferredContext ctx = from.getContext();

    if (!from.isValid()) {
      throw new QueryBuildException(QueryResult.of(request, from));
    }

    // SELECT. Build the select statement.
    ColumnName aggregationColumn = request.getAggregationColumn();
    Optional<String> aggregationFunc = request.getAggregationFunc();

    Select select;
    // Select all case has no aggregation.
    if (request.getSelectAll().isPresent()) {
      select = QueryUtil.selectList(new AllColumns());
    } else {
      // If not select all, then form the aggregation function.
      SelectItem aggr;
      if (aggregationColumn.getColumn().isPresent()) {
        aggr = new SingleColumn(QueryUtil.functionCall(
            aggregationFunc.get(),
            new QualifiedNameReference(QualifiedName.of(
                ctx.getAggregationPrefix().get(),
                aggregationColumn.getColumn().get()))));
      } else {
        aggr = new SingleColumn(QueryUtil.functionCall(aggregationFunc.get()));
      }

      // If a group by is present, then also select the column that we are
      // grouping by, for easy group reference.
      if (request.getGroupByColumn().getColumn().isPresent()) {
        QualifiedNameReference groupByName =
            new QualifiedNameReference(QualifiedName.of(
                ctx.getGroupByPrefix().get(),
                request.getGroupByColumn().getColumn().get()));
        select = QueryUtil.selectList(new SingleColumn(groupByName), aggr);
      } else {
        // Otherwise select only the aggregate.
        select = QueryUtil.selectList(aggr);
      }
    }

    // WHEREs. Build the where clauses.
    List<ColumnName> comparisonColumns = request.getComparisonColumns();
    List<Optional<Type>> comparators = request.getComparators();
    List<Optional<String>> comparisonValues = request.getComparisonValues();

    // Where clauses are represented as a list of comparison expressions.
    // We add a comparison expression for each comparison column we have.
    List<Expression> whereClauses = new ArrayList<>();
    for (int i = 0; i < comparisonColumns.size(); i++) {
      ComparisonExpression.Type comparator = comparators.get(i).get();
      Expression column = new QualifiedNameReference(QualifiedName.of(
          ctx.getComparisonPrefix(i).get(),
          comparisonColumns.get(i).getColumn().get()));
      Expression value =
          (comparisonColumns.get(i).getType() == ColumnType.NUMBER)
              ? new LongLiteral(comparisonValues.get(i).get())
              : new StringLiteral(comparisonValues.get(i).get());

      whereClauses.add(new ComparisonExpression(comparator, column, value));
    }

    // GROUP-BYs. Build the group by clause.
    ColumnName groupByColumn = request.getGroupByColumn();

    // Group by in the AST is a list of grouping elements, but we'll only
    // support one here i.e. a single group by.
    List<GroupingElement> groupBy = new ArrayList<>();
    QualifiedNameReference groupByName = null;
    if (groupByColumn.getColumn().isPresent()) {
      groupByName = new QualifiedNameReference(QualifiedName.of(
          ctx.getGroupByPrefix().get(), groupByColumn.getColumn().get()));
      groupBy.add(new SimpleGroupBy(ImmutableList.of(groupByName)));
    }

    // Put everything together to build the AST representing this query.
    return QueryUtil.simpleQuery(
        select,
        from.render(),
        java.util.Optional.ofNullable(
            // Combine the list of where clauses into a valid tree structure.
            logicallyCombineWhereClauses(
                whereClauses, request.getComparisonBinaryOperators(), 0)),
        groupBy,
        java.util.Optional.empty(),
        ImmutableList.of(),
        java.util.Optional.empty());
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
}
