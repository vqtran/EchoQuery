package echoquery.sql;

public class ForeignKey {
  private String sourceColumn;
  private String sourceTable;
  private String destinationColumn;
  private String destinationTable;
  
  
  public ForeignKey(String sourceColumn, String sourceTable, 
      String destinationColumn, String destinationTable) {
    this.sourceColumn = sourceColumn;
    this.sourceTable = sourceTable;
    this.destinationColumn = destinationColumn;
    this.destinationTable = destinationTable;
  }
  
  public String getSourceColumn() {
    return this.sourceColumn;
  }

  public String destinationColumn() {
    return this.destinationColumn;
  }
  
  public String getDestinationTable() {
    return this.destinationTable;
  }
  
  public String getSourceTable() {
    return this.sourceTable;
  }

}
