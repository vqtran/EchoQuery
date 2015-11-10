package echoquery.sql.joins;

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
  public String render() {
    return table;
  }
}
