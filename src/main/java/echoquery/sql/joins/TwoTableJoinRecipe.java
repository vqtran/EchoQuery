package echoquery.sql.joins;

import echoquery.sql.model.ForeignKey;

public class TwoTableJoinRecipe implements JoinRecipe {
  private String sourceColumn;
  private String sourceTable;
  private String destinationColumn;
  private String destinationTable;
  
  
  public TwoTableJoinRecipe(String sourceColumn, String sourceTable, 
      String destinationColumn, String destinationTable) {
    this.sourceColumn = sourceColumn;
    this.sourceTable = sourceTable;
    this.destinationColumn = destinationColumn;
    this.destinationTable = destinationTable;
  }

  public TwoTableJoinRecipe(ForeignKey foreignKey) {
    this.sourceColumn = foreignKey.getSourceColumn();
    this.sourceTable = foreignKey.getSourceTable();
    this.destinationColumn = foreignKey.getDestinationColumn();
    this.destinationTable = foreignKey.getDestinationTable();
  }
  
  @Override
  public boolean isValid() {
    return true;
  }
  
  @Override
  public String render() {
    return this.sourceTable + " JOIN " + this.destinationTable + " ON "
        + this.sourceTable + "." + this.sourceColumn + "=" + this.destinationTable 
        + "." + this.destinationColumn;
    
  }

}
