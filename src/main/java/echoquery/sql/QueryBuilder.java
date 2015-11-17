package echoquery.sql;

import java.util.Optional;

import com.amazon.speech.slu.Intent;
import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.StringLiteral;

import echoquery.utils.SlotUtil;

public class QueryBuilder {

  private Select select;
  private Relation from;
  private Optional<Expression> where;

  public QueryBuilder() {}

  public QueryBuilder(Select select, Relation from) {
    this.select = select;
    this.from = from;
    this.where = Optional.empty();
  }

  public QueryBuilder(Intent intent) {

  }

  public QueryBuilder select(Select select) {
    this.select = select;
    return this;
  }

  public QueryBuilder from(Relation from) {
    this.from = from;
    return this;
  }

  public QueryBuilder where(Expression where) {
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
    String table = intent.getSlot(SlotUtil.TABLE_NAME).getValue();

    QueryBuilder builder = new QueryBuilder()
        .select(QueryUtil.selectList(QueryUtil.functionCall("COUNT")))
        .from(QueryUtil.table(new QualifiedName(table)));

    String column = intent.getSlot(SlotUtil.COLUMN_NAME).getValue();
    String colVal= intent.getSlot(SlotUtil.COLUMN_VALUE).getValue();
    String colNum = intent.getSlot(SlotUtil.COLUMN_NUMBER).getValue();
    String equals = intent.getSlot(SlotUtil.EQUALS).getValue();
    String greater = intent.getSlot(SlotUtil.GREATER_THAN).getValue();
    String less = intent.getSlot(SlotUtil.LESS_THAN).getValue();

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
          QueryUtil.nameReference(column), new StringLiteral(match)));
    }

    return builder;
  }
}
