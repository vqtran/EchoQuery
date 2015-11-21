package echoquery.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import echoquery.sql.joins.InferredContext;
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

  private final List<String> tables;

  private static SchemaInferrer inferrer = new SchemaInferrer();

  public static SchemaInferrer getInstance() {
    return inferrer;
  }

  private SchemaInferrer() {
    Connection conn = SingletonConnection.getInstance();
    columnToTable = new HashMap<>();
    tableToForeignKeys = new HashMap<>();
    tables = new ArrayList<>();

    try {
      DatabaseMetaData md = conn.getMetaData();
      // find all the table names
      tables.addAll(findColumnNames(md));
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
   * @param columns the columns of interest
   * @param sourceTable the table of interest
   * @param destinationTable the table being joined
   * @return an ordered list of which table each column belongs to. the ith
   *         table in prefixes corresponds to the ith column in columns.
   */
  private List<String> inferPrefixesForList(List<String> columns,
      String sourceTable, String destinationTable) {
    List<String> prefixes = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      Set<String> tables = columnToTable.get(column);
      if (tables.contains(sourceTable)) {
        prefixes.add(sourceTable);
      } else {
        prefixes.add(destinationTable);
      }
    }
    return prefixes;
  }

  private String inferPrefixForSingle(String column, String sourceTable, String destinationTable) {
    return inferPrefixesForList(Arrays.asList(column), sourceTable, destinationTable).get(0);
  }

  private InferredContext inferPrefixes(String aggregation, List<String> comparisons,
      String table, String destinationTable) {
    List<String> comparisonPrefixes = inferPrefixesForList(comparisons, table, destinationTable);
    String aggregationPrefix = aggregation != null ?
        inferPrefixForSingle(aggregation, table, destinationTable) :
        null;
    return new InferredContext(aggregationPrefix, comparisonPrefixes);
  }

  /**
   *
   * @param table the table that we want to aggregate over / select from
   * @param column the column that were filtering on
   * @return a JoinRecipe that can create the table to filter one and select
   *    from
   */
  public JoinRecipe infer(String table, String aggregation, List<String> comparisons) {
    // initially we can join with any table
    Set<String> criteriaTables = new HashSet<>(tables);

    // aggregate all the columns we are interested in
    List<String> columns = new ArrayList<>(comparisons);
    columns.add(aggregation);
    columns.removeAll(Collections.singleton(null));

    // for each column we are interested, we need to filter down only tables 
    // that contain it (ignoring columns that already exist in our base table)
    for (String column : columns) {
      Set<String> newCriteria = columnToTable.getOrDefault(column, new HashSet<String>());
      if (newCriteria.contains(table)) continue;
      criteriaTables.retainAll(newCriteria);
    }
    // if all the columns could be in our base table
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
         return new TwoTableJoinRecipe(foreignKey, inferPrefixes(aggregation, comparisons, table, destinationTable));
        }
      }
      return new InvalidJoinRecipe();
    } else {
      // we can't find the column, return a totally null join recipe
      return new InvalidJoinRecipe();
    }
  }
}