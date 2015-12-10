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

  public InferredContext getContext();
}
