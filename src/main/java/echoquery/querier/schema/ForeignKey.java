package echoquery.querier.schema;

public class ForeignKey {
  private String sourceColumn;
  private String sourceTable;
  private String destinationColumn;
  private String destinationTable;

  public ForeignKey(String sourceColumn,
      String sourceTable, String destinationColumn, String destinationTable) {
    this.sourceColumn = sourceColumn;
    this.sourceTable = sourceTable;
    this.destinationColumn = destinationColumn;
    this.destinationTable = destinationTable;
  }

  public String getSourceColumn() {
    return sourceColumn;
  }

  public String getDestinationColumn() {
    return destinationColumn;
  }

  public String getDestinationTable() {
    return destinationTable;
  }

  public String getSourceTable() {
    return sourceTable;
  }
}
