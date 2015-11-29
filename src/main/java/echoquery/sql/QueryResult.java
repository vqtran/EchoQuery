package echoquery.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;

import echoquery.utils.SlotUtil;
import echoquery.utils.TranslationUtils;

/**
 * The QueryResult class contains a status of a query request and a message for
 * the end user.
 */

public class QueryResult {
  enum Status {
    SUCCESS,
    REPAIR_REQUEST,
    FAILURE
  }
  private final Status status;
  private final String message;

  public QueryResult(Status status, String message) {
    this.status = status;
    this.message = message;
  }

  public Status getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  /**
   * Builds a QueryResult out of a JDBC result set, and original request object.
   * @param request
   * @param result
   * @return A successful result where the message is the result of the query
   *    is in end-user natural language.
   */
  public static QueryResult of(QueryRequest request, ResultSet result) {
    // Get the answer as a double to cover decimal values.
    double value;
    try {
      result.first();
      value = result.getDouble(1);
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }

    StringBuilder translation = new StringBuilder();

    /**
     * Visit each node in the tree and convert it to natural language. Results
     * in a sentence form of the query.
     */
    AstVisitor<Void, Void> translator =
        new DefaultTraversalVisitor<Void, Void>() {

      @Override
      protected Void visitQuerySpecification(QuerySpecification node, Void v) {
        process(node.getSelect(), null);

        if (node.getFrom().isPresent()) {
          // Use what the user referenced, ignoring any complicated join
          // inference we did to get it to work.
          translation.append("in the ")
              .append(request.getFromTable().get())
              .append(" table");
        }
        if (node.getWhere().isPresent()) {
            translation.append(" where the value in ");
            process(node.getWhere().get(), null);
        }
        for (Expression groupingElement : node.getGroupBy()) {
            process(groupingElement, null);
        }
        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), null);
        }
        for (SortItem sortItem : node.getOrderBy()) {
            process(sortItem, null);
        }
        return null;
      }

      @Override
      public Void visitFunctionCall(FunctionCall node, Void v) {
        String aggregation = request.getAggregationFunc().get();
        if (aggregation.equals("COUNT")) {
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
          process(node.getArguments().get(0), null);
          translation.append(" column ");
        }
        return null;
      }

      @Override
      public Void visitLogicalBinaryExpression(
          LogicalBinaryExpression node, Void v) {
        process(node.getLeft(), null);
        switch (node.getType()) {
          case AND:
            translation.append(" and ");
            break;
          case OR:
            translation.append(" or ");
            break;
        }
        process(node.getRight(), null);
        return null;
      }

      @Override
      public Void visitComparisonExpression(ComparisonExpression node, Void v) {
        translation.append("the ");
        process(node.getLeft(), null);
        translation.append(" column is")
            .append(SlotUtil.comparisonTypeToEnglish(node.getType()));
        process(node.getRight(), null);
        return null;
      }

      @Override
      public Void visitQualifiedNameReference(
          QualifiedNameReference node, Void v) {
        translation.append(node.getSuffix());
        return null;
      }

      @Override
      public Void visitStringLiteral(StringLiteral node, Void v) {
        translation.append(node.getValue());
        return null;
      }

      @Override
      public Void visitLongLiteral(LongLiteral node, Void v) {
        translation.append(TranslationUtils.convert(node.getValue()));
        return null;
      }
    };
    translator.process(request.getQuery(), null);

    if (!request.getAggregationFunc().get().equals("COUNT")) {
      translation.append(" is ").append(TranslationUtils.convert(value));
    }
    translation.append(".");
    return new QueryResult(Status.SUCCESS, translation.toString());
  }
}

