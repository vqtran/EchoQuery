package echoquery.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import echoquery.utils.TranslationUtils;

public class QueryResult {
  private static final Logger log = LoggerFactory.getLogger(QueryResult.class);

  private final QueryRequest request;
  private final ResultSet result;

  public QueryResult(QueryRequest request, ResultSet result) {
    this.request = request;
    this.result = result;
  }

  public QueryRequest getQueryRequest() {
    return request;
  }

  public ResultSet getResult() {
    return result;
  }

  public String toEnglish() {
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
              .append(request.getFromTable())
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
        String aggregation = request.getAggregationFunc();
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
          return null;
        }

        String longForm = "";
        switch (aggregation) {
          case "AVG":
            longForm = "average";
            break;
          case "SUM":
            longForm = "total";
            break;
          case "MIN":
            longForm = "minimum value";
            break;
          case "MAX":
            longForm = "maximum value";
            break;
        }
        translation.append("The ").append(longForm).append(" of the ");
        process(node.getArguments().get(0), null);
        translation.append(" column ");

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
        translation.append(" column is");
        switch (node.getType()) {
          case EQUAL:
            translation.append(" equal to ");
            break;
          case NOT_EQUAL:
            translation.append(" not equal to ");
            break;
          case LESS_THAN:
            translation.append(" less than ");
            break;
          case LESS_THAN_OR_EQUAL:
            translation.append(" less than or equal to ");
            break;
          case GREATER_THAN:
            translation.append(" greater than ");
            break;
          case GREATER_THAN_OR_EQUAL:
            translation.append(" greater than or equal to ");
            break;
          case IS_DISTINCT_FROM:
            translation.append(" is distinct from ");
            break;
        }
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

    if (!request.getAggregationFunc().equals("COUNT")) {
      translation.append(" is ").append(TranslationUtils.convert(value));
    }
    translation.append(".");
    return translation.toString();
  }
}

