package echoquery.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.Table;

import echoquery.utils.TranslationUtils;

public class QueryResult {

  private final Query query;
  private final ResultSet result;

  public QueryResult(Query query, ResultSet result) {
    this.query = query;
    this.result = result;
  }

  public Query getQuery() {
    return query;
  }

  public ResultSet getResult() {
    return result;
  }

  public String toEnglish() {
    StringBuilder translation = new StringBuilder();
    AstVisitor<Void, Boolean> translator =
        new DefaultTraversalVisitor<Void, Boolean>() {

      @Override
      public Void visitFunctionCall(FunctionCall node, Boolean capture) {
        int value;
        try {
          result.first();
          value = result.getInt(1);
        } catch (SQLException e) {
          e.printStackTrace();
          return null;
        }
        switch (node.getName().toString()) {
          case "count":
            if (value == 1) {
              translation.append("There is ")
                .append(TranslationUtils.convert(value))
                .append(" row ");
            } else {
              translation.append("There are ")
                .append(TranslationUtils.convert(value))
                .append(" rows ");
            }
            break;
          default:
            break;
        }
        return null;
      }

      @Override
      public Void visitTable(Table node, Boolean capture) {
        translation.append("in the ")
            .append(node.getName().toString())
            .append(" table");
        return null;
      }

      @Override
      public Void visitComparisonExpression(
          ComparisonExpression node, Boolean capture) {
        translation.append(" where the value in the ");
        process(node.getLeft(), true);
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
        process(node.getRight(), true);
        return null;
      }

      @Override
      public Void visitQualifiedNameReference(
          QualifiedNameReference node, Boolean capture) {
        if (capture) {
          translation.append(node.getName().toString());
        }
        return null;
      }

      @Override
      public Void visitStringLiteral(StringLiteral node, Boolean capture) {
        if (capture) {
          translation.append(node.getValue());
        }
        return null;
      }
    };
    translator.process(query, false);
    translation.append(".");
    return translation.toString();
  }
}
