package echoquery.sql;

import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author gabe
 * Knows the different columns on each table and the connections between tables
 *
 */
public class StarSchemaStore {
  private final String centerTable;
  private final Map<String, HashSet<String>> columnToTable;
  private final Map<String, List<ForeignKey>> tableForeignKeys;

  public StarSchemaStore(Connection conn) {
    // find all the foreign keys for each table
    
    // find all the columns in a given table
  }
  
  /**
   * 
   * @param table the table that we want to aggregate over / select from
   * @param column
   */
  public infer(String table, String column) {
    // if the inputted table is the center table
      // find which table has the specific column in columnToTable
      // find the foreign key to the center table
    // otherwise, if the column is not in the table give up! if it is youre good
  }

}
