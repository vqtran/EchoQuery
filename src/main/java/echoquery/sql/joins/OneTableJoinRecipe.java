package echoquery.sql.joins;

import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Relation;

public class OneTableJoinRecipe implements JoinRecipe {

  private String table;

  public OneTableJoinRecipe(String table) {
    this.table = table;
  }

  @Override
  public boolean isValid() {
    return true;
  }

  @Override
  public Relation render() {
    return QueryUtil.table(new QualifiedName(table));
  }

  @Override
  public String getComparisonPrefix(int index) {
    return table;
  }

  @Override
  public String getAggregationPrefix() {
    return table;
  }
}