package echoquery.sql.joins;

public class InvalidJoinRecipe implements JoinRecipe {

  @Override
  public boolean isValid() {
    return false;
  }

  @Override
  public String render() {
    return "";
  }
}
