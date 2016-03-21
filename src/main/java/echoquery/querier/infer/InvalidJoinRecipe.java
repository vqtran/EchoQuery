package echoquery.querier.infer;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.facebook.presto.sql.tree.Relation;

public class InvalidJoinRecipe implements JoinRecipe {
  // There's only two types of errors possible for schema inference.
  public enum Error {
    AMBIGUOUS_TABLE_FOR_COLUMN,
    MISSING_FOREIGN_KEY,
  }

  private Error reason;
  private Set<String> possibleTables;
  private String invalidColumn;
  private InferredContext ctx;

  private InvalidJoinRecipe(
      Error reason,
      @Nullable Set<String> possibleTables,
      @Nullable String invalidColumn,
      @Nullable InferredContext ctx) {
    this.reason = reason;
    this.possibleTables = possibleTables;
    this.invalidColumn = invalidColumn;
    this.ctx = ctx;
  }

  public static InvalidJoinRecipe ambiguousTableForColumn(
      Set<String> possibleTables, InferredContext ctx) {
    return new InvalidJoinRecipe(
        Error.AMBIGUOUS_TABLE_FOR_COLUMN, possibleTables, null, ctx);
  }

  public static InvalidJoinRecipe missingForeignKey(String invalidColumn) {
    return new InvalidJoinRecipe(
        Error.MISSING_FOREIGN_KEY, null, invalidColumn, null);
  }

  public Error getReason() {
    return reason;
  }

  @Override
  public InferredContext getContext() {
    return ctx;
  }

  @Override
  public boolean isValid() {
    return false;
  }

  @Override
  public Relation render() {
    return null;
  }

  @Override
  public List<String> getPossibleTables() {
    return Arrays.asList(possibleTables.toArray(new String[]{}));
  }

  @Override
  public String getInvalidColumn() {
    return invalidColumn;
  }
}
