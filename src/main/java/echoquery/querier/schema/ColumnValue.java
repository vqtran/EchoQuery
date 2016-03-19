package echoquery.querier.schema;

public class ColumnValue {
  private String value;
  private boolean isString;

  public ColumnValue(String value, boolean isString) {
    this.value = value; 
    this.isString = isString;
  }

  public String render() {
    if (isString) {
      return '"' + value.toString() + '"';
    } else {
      return value.toString();
    }
  }
}
