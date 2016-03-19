package echoquery.querier.infer;

import java.util.List;

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
   * FOR VALID JOINRECIPES
   */

  /**
   * Render the join into a Relation which can be selected from in the AST.
   * @return Relation that formulates the join(s) needed.
   */
  public Relation render();

  public InferredContext getContext();

  /**
   * FOR INVALID JOINRECIPES
   */

  /**
   * @return The error reason.
   */
  public InvalidJoinRecipe.Error getReason();

  /**
   * @return Return the possible tables for an ambiguous column.
   */
  public List<String> getPossibleTables();

  /**
   * @return Return the invalid column which is missing a foreign key connection
   */
  public String getInvalidColumn();
}
