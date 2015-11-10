package echoquery.sql.joins;

public interface JoinRecipe {
  public boolean isValid();
  public String render();
}
