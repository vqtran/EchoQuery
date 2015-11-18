package echoquery.sql.joins;

import com.facebook.presto.sql.tree.Relation;

public class InvalidJoinRecipe implements JoinRecipe {

  @Override
  public boolean isValid() {
    return false;
  }

  @Override
  public Relation render() {
    return null;
  }

  @Override
  public String wherePrefix(int index) {
    return null;
  }
}
