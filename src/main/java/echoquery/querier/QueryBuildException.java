package echoquery.querier;

public class QueryBuildException extends Exception {

  private static final long serialVersionUID = 1L;
  private QueryResult result;

  public QueryBuildException(QueryResult result) {
    this.result = result;
  }

  public QueryResult getResult() {
    return result;
  }
}
