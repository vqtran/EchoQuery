package echoquery.sql;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.slu.Intent;
import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.StringLiteral;

import echoquery.sql.joins.JoinRecipe;
import echoquery.sql.joins.OneTableJoinRecipe;
import echoquery.utils.SlotNames;

public class QueryBuilder {

  private static final Logger log =
      LoggerFactory.getLogger(QueryBuilder.class);
  private Select select;
  private Relation from;
  private Optional<Expression> where;

  private QueryBuilder() {}

  private QueryBuilder(Select select, Relation from) {
    this.select = select;
    this.from = from;
    this.where = Optional.empty();
  }

  private QueryBuilder(Intent intent) {

  }

  private QueryBuilder select(Select select) {
    this.select = select;
    return this;
  }

  private QueryBuilder from(Relation from) {
    this.from = from;
    return this;
  }

  private QueryBuilder where(Expression where) {
    this.where = Optional.of(where);
    return this;
  }

  public Query build() {
    if (where != null && where.isPresent()) {
      return QueryUtil.simpleQuery(select, from, where.get());
    } else {
      return QueryUtil.simpleQuery(select, from);
    }
  }

  public static QueryBuilder of(Intent intent) {
    String table = intent.getSlot(SlotNames.TABLE_NAME).getValue();
    SchemaInferrer inferrer = SchemaInferrer.getInstance();

    String column = intent.getSlot(SlotNames.COLUMN_NAME).getValue();
    String colVal= intent.getSlot(SlotNames.COLUMN_VALUE).getValue();
    String colNum = intent.getSlot(SlotNames.COLUMN_NUMBER).getValue();
    String equals = intent.getSlot(SlotNames.EQUALS).getValue();
    String greater = intent.getSlot(SlotNames.GREATER_THAN).getValue();
    String less = intent.getSlot(SlotNames.LESS_THAN).getValue();

    JoinRecipe from;
    if (column != null) {
      from = inferrer.infer(table, column != null ? column : "");
    } else {
      from = new OneTableJoinRecipe(table);
    }

    QueryBuilder builder = new QueryBuilder()
        .select(QueryUtil.selectList(QueryUtil.functionCall("COUNT")))
        .from(from.render());

    String match = null;
    if (column != null) {
      match = (colVal == null) ? colNum : colVal;
      ComparisonExpression.Type comparison = null;
      if (equals != null) {
        comparison = ComparisonExpression.Type.EQUAL;
      }
      if (greater != null) {
        comparison = ComparisonExpression.Type.GREATER_THAN;
      }
      if (less != null) {
        comparison = ComparisonExpression.Type.LESS_THAN;
      }
      builder.where(new ComparisonExpression(comparison,
          new QualifiedNameReference(
              QualifiedName.of(from.wherePrefix(), column)),
          new StringLiteral(match)));
    }

    return builder;
  }
}