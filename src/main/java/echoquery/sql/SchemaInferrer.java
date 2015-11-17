package echoquery.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import echoquery.sql.joins.InvalidJoinRecipe;
import echoquery.sql.joins.JoinRecipe;
import echoquery.sql.joins.OneTableJoinRecipe;
import echoquery.sql.joins.TwoTableJoinRecipe;
import echoquery.sql.model.ForeignKey;

/**
 * Knows the different columns on each table and the connections between tables
 */
public class SchemaInferrer {

  private static final Logger log =
      LoggerFactory.getLogger(SchemaInferrer.class);

  // column name maps to set of table names that contain a column with that name
  private final Map<String, Set<String>> columnToTable;

  // table name maps to list of foreign keys that exist in the table
  // and the primary keys that they map to
  private final Map<String, List<ForeignKey>> tableToForeignKeys;

  private static SchemaInferrer inferrer = new SchemaInferrer();

  public static SchemaInferrer getInstance() {
    return inferrer;
  }

  private SchemaInferrer() {
    Connection conn = SingletonConnection.getInstance();
    columnToTable = new HashMap<>();
    tableToForeignKeys = new HashMap<>();

    try {
      DatabaseMetaData md = conn.getMetaData();
      // find all the table names
      List<String> tables = findColumnNames(md);
      for (String table : tables) {
        // fill in columnToTable
        adjustColumnToTable(table, md);
        // fill in table to foreign keys
        adjustTableToForeignKeys(table, md);
      }
    } catch (SQLException e) {
      log.info("Error generating the StarSchemaStore");
      e.printStackTrace();
    }
  }

  private void adjustTableToForeignKeys(String table, DatabaseMetaData md)
      throws SQLException {
    ResultSet foreignKeys = md.getImportedKeys(null, null, table);
    while (foreignKeys.next()) {
      String fkTableName = foreignKeys.getString("FKTABLE_NAME");
      String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
      String pkTableName = foreignKeys.getString("PKTABLE_NAME");
      String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");
      List<ForeignKey> previousList = tableToForeignKeys.getOrDefault(
          fkTableName, new ArrayList<ForeignKey>());
      previousList.add(
          new ForeignKey(fkColumnName, fkTableName, pkColumnName, pkTableName));
      tableToForeignKeys.put(fkTableName, previousList);
    }
  }

  private void adjustColumnToTable(String table, DatabaseMetaData md)
      throws SQLException {
    ResultSet columns = md.getColumns(null, null, table, null);
    while (columns.next()) {
      String col = columns.getString("COLUMN_NAME");
      Set<String> previousSet =
          columnToTable.getOrDefault(col, new HashSet<String>());
      previousSet.add(table);
      columnToTable.put(col, previousSet);
    }
  }

  private List<String> findColumnNames(DatabaseMetaData md)
      throws SQLException {
    List<String> tables = new ArrayList<>();
    ResultSet rs = md.getTables(null, null, "%", null);
    while (rs.next()) {
      tables.add(rs.getString(3));
    }
    return tables;
  }

  /**
   *
   * @param table the table that we want to aggregate over / select from
   * @param column the column that were filtering on
   * @return a JoinRecipe that can create the table to filter one and select
   *    from
   */
  public JoinRecipe infer(String table, String column) {
    // if the inputted table is the center table
    // find which table has the specific column in columnToTable
    // find the foreign key to the center table
    // otherwise, if the column is not in the table give up! if it is you're
    // good
    Set<String> criteriaTables =
        columnToTable.getOrDefault(column, new HashSet<String>());
    // if the column could be in the table we're querying on
    if (criteriaTables.contains(table)) {
      // we don't need to join
      return new OneTableJoinRecipe(table);
    } else if (criteriaTables.size() >= 1) {
      // choose the first one that connects to our table
      List<ForeignKey> foreignKeys =
          tableToForeignKeys.getOrDefault(table, new ArrayList<ForeignKey>());
      for (ForeignKey foreignKey : foreignKeys) {
        String destinationTable = foreignKey.getDestinationTable();
        if (criteriaTables.contains(destinationTable)) {
         return new TwoTableJoinRecipe(foreignKey);
        }
      }
      return new InvalidJoinRecipe();
    } else {
      // we can't find the column, return a totally null join recipe
      return new InvalidJoinRecipe();
    }
  }
}