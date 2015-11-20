package echoquery.sql;

import java.util.Arrays;
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
import echoquery.utils.SlotUtil;

public class QueryBuilder {

  private static final Logger log = LoggerFactory.getLogger(QueryBuilder.class);
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
    String table = intent.getSlot(SlotUtil.TABLE_NAME).getValue();
    SchemaInferrer inferrer = SchemaInferrer.getInstance();

    String aggregate = intent.getSlot(SlotUtil.AGGREGATE).getValue();
    String aggregationColumn =
        intent.getSlot(SlotUtil.AGGREGATION_COLUMN).getValue();
    String comparisonColumn =
        intent.getSlot(SlotUtil.COMPARISON_COLUMN).getValue();
    String comparator = intent.getSlot(SlotUtil.COMPARATOR).getValue();
    String colVal= intent.getSlot(SlotUtil.COLUMN_VALUE).getValue();
    String colNum = intent.getSlot(SlotUtil.COLUMN_NUMBER).getValue();

    QueryBuilder builder = new QueryBuilder();

    JoinRecipe from;
    if (comparisonColumn == null) {
      from = new OneTableJoinRecipe(table);
    } else {
      from = inferrer.infer(table, aggregationColumn, Arrays.asList(comparisonColumn));
    }

    builder.from(from.render());

    Expression aggregationFunc;
    if (aggregationColumn == null) {
      aggregationFunc = QueryUtil.functionCall(
          SlotUtil.getAggregateFunction(aggregate));
    } else {
      aggregationFunc = QueryUtil.functionCall(
          SlotUtil.getAggregateFunction(aggregate),
          new QualifiedNameReference(
              QualifiedName.of(from.getAggregationPrefix(), aggregationColumn)));
    }
    builder.select(QueryUtil.selectList(aggregationFunc));


    String match = null;
    if (comparisonColumn != null) {
      match = (colVal == null) ? colNum : colVal;
      builder.where(new ComparisonExpression(
          SlotUtil.getComparisonType(comparator),
          new QualifiedNameReference(
              QualifiedName.of(from.getComparisonPrefix(0), comparisonColumn)),
          new StringLiteral(match)));
    }

    return builder;
  }
}