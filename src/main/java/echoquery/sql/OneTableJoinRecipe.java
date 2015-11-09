package echoquery.sql;

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
    // TODO Auto-generated method stub
    return table;
  }

}
