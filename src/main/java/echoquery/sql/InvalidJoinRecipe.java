package echoquery.sql;

public class InvalidJoinRecipe implements JoinRecipe {

  public InvalidJoinRecipe() {
    
  }
  
  @Override
  public boolean isValid() {
    return false;
  }
  
  @Override
  public String render() {
    return "";
  }

}
