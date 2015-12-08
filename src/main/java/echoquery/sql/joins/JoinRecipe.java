package echoquery.sql.joins;

import com.facebook.presto.sql.tree.Relation;

/**
 * This class holds information needed to render and refer to an inferred join.
 */
public interface JoinRecipe {
  /**
   * @return True if we found a valid join.
   */
  public boolean isValid();

  /**
   * Render the join into a Relation which can be selected from in the AST.
   * @return Relation that formulates the join(s) needed.
   */
  public Relation render();

  /**
   * @return The table name that the aggregation column belongs to.
   */
  public String getAggregationPrefix();

  /**
   * @param index
   * @return The table name that the index-th comparison column belongs to.
   */
  public String getComparisonPrefix(int index);

  /**
   * @return The table name that the group by column belongs to.
   */
  public String getGroupByPrefix();
}
