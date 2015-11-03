package echoquery.sql;

public class ColumnValue {
  private String value;
  private Boolean isString;

  public ColumnValue(String value, Boolean isString) {
    this.value = value; 
    this.isString = isString;
  }
  
  public String render() {
    if (this.isString) {
      return '"' + this.value.toString() + '"';
    } else {
      return this.value.toString();
    }
  }
}
