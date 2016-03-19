package echoquery.querier.translate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import org.json.JSONException;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableSet;

import echoquery.querier.QueryRequest;
import echoquery.querier.ResultTable;
import echoquery.querier.infer.InferredContext;
import echoquery.querier.infer.JoinRecipe;
import echoquery.querier.infer.SchemaInferrer;
import echoquery.utils.SlotUtil;

/**
 * Translates a ResultTable given the QueryRequest and Query ASt into a natural
 * language expression of the result.
 */
public class ResultTranslator {
  private SchemaInferrer inferrer;

  public ResultTranslator(SchemaInferrer inferrer) {
    this.inferrer = inferrer;
  }

  /**
   * @param result ResultTable created from the ResultSet
   * @param request QueryRequest used to get the result
   * @param query Query AST used to get the Result
   * @return The natural language string conveying the ResultTable.
   * @throws JSONException
   * @throws SQLException
   */
  public String translate(ResultTable result, QueryRequest request, Query query)
      throws JSONException, SQLException {
    JoinRecipe from = inferrer.infer(
        request.getFromTable().get(),
        request.getAggregationColumn(),
        request.getComparisonColumns(),
        request.getGroupByColumn());
    InferredContext ctx = from.getContext();

    /**
     * First compute the column names that we'll use to look up things in
     * the result table in the table_name.column_name format that ResultTable
     * uses.
     */
    String groupByTable = "";
    String groupByCol = "";
    String groupColName = "";
    String aggregateColName;

    if (request.getAggregationFunc().isPresent()) {
      if (request.getAggregationFunc().get().equals("COUNT")) {
        aggregateColName = "count(*)";
      } else {
        aggregateColName = request.getAggregationFunc().get().toLowerCase()
            + "("
            + String.join(".",
                ctx.getAggregationPrefix().get(),
                request.getAggregationColumn().getColumn().get())
            + ")";
      }
    } else {
      aggregateColName = "";
    }

    if (request.getGroupByColumn().getColumn().isPresent()) {
      groupByTable = ctx.getGroupByPrefix().get();
      groupByCol = request.getGroupByColumn().getColumn().get();
      groupColName = String.join(".", groupByTable, groupByCol);
    }

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
        // If its SELECT * then we give a special message.
        if (request.getSelectAll().isPresent()) {
          if (result.numRows() == 0) {
            translation.append("There are zero rows ");
          } else if (result.numRows() == 1) {
            translation.append("Here's the one row ");
          } else {
            translation.append("Here are all ")
                .append(TranslationUtils.convert(result.numRows()))
                .append(" rows ");
          }
        } else {
          // Otherwise we use the one corresponding to the aggregation.
          // By default do not capture any qualified names in select clauses.
          process(node.getSelect(), false);
        }

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
            if (request.getSelectAll().isPresent() &&
                groupingElement instanceof SimpleGroupBy) {
              translation.append(" grouped by ");
              ImmutableSet.copyOf(
                  ((SimpleGroupBy) groupingElement).getColumnExpressions())
                      .forEach((c) -> process(c, true));
            }
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
          double value = result.getDouble(aggregateColName, 0);
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
    translator.process(query, true);

    /**
     * At this point the body of the query has been translated, but if there was
     * aggregation with a group by, we need to append one or more results to the
     * end now.
     */
    if (request.getAggregationFunc().isPresent()) {
      // If it's count and it only had a single value, we're all set.
      if (request.getAggregationFunc().get().equals("COUNT")) {
        // Otherwise if its count and a group by, we then have to say the group
        // of the first result we already stated, as well as add on the rest of
        // the results by group.
        if (result.numRows() > 1) {
          translation.append(" for the ");
          if (isAmbiguousWithoutTable(groupByCol, inferrer)) {
            translation.append(groupByTable).append(" ");
          }
          translation
              .append(groupByCol).append(" ")
              .append(result.getString(groupColName, 0));

          for (int i = 1; i < result.numRows(); i++) {
            translation.append(", ");
            if (i == result.numRows() - 1) {
              translation.append("and ");
            }
            translation
                .append(TranslationUtils.convert(
                    result.getDouble(aggregateColName, i)))
                .append((result.getDouble(aggregateColName, i) == 1)
                    ? " row for " : " rows for ")
                .append(result.getString(groupColName, i));
          }
        }
      } else {
        // For all other aggregations, if there's only a single value we simply
        // tack it on.
        if (result.numRows() == 1) {
          translation.append(" is ")
              .append(TranslationUtils.convert(
                  result.getDouble(aggregateColName, 0)));
        } else {
          // Otherwise we have to tack on all of the remaining values.
          for (int i = 0; i < result.numRows(); i++) {
            if (i == 0) {
              translation.append(" is ")
                  .append(TranslationUtils.convert(
                      result.getDouble(aggregateColName, i)))
                  .append(" for the ");
              // Include what table group by belonged to only if ambiguous
              // otherwise.
              if (isAmbiguousWithoutTable(
                  request.getGroupByColumn().getColumn().get(), inferrer)) {
                translation
                    .append(ctx.getGroupByPrefix().get())
                    .append(" ");
              }
              translation.append(request.getGroupByColumn().getColumn().get())
                  .append(" ")
                  .append(result.getString(groupColName, i));
            } else {
              translation.append(", ");
              if (i == result.numRows() - 1) {
                translation.append("and ");
              }
              translation
                  .append(TranslationUtils.convert(
                      result.getDouble(aggregateColName, i)))
                  .append(" for ")
                  .append(result.getString(groupColName, i));
            }
          }
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

}
