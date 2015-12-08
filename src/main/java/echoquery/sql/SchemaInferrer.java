package echoquery.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import echoquery.sql.joins.InferredContext;
import echoquery.sql.joins.InvalidJoinRecipe;
import echoquery.sql.joins.JoinRecipe;
import echoquery.sql.joins.OneTableJoinRecipe;
import echoquery.sql.joins.TwoTableJoinRecipe;
import echoquery.sql.model.ForeignKey;

/**
 * The SchemaInferrer class knows the different columns on each table and the
 * connections between tables, and is able to generate join specifications to
 * resolve ambiguous column references.
 */
public class SchemaInferrer {

  private static final Logger log =
      LoggerFactory.getLogger(SchemaInferrer.class);

  /**
   * List of all tables.
   */
  private final List<String> tables;

  /**
   *  Column name maps to set of table names that contain a column with that
   *  name.
   */
  private final Map<String, Set<String>> columnToTable;

  /**
   * Table name maps to list of foreign keys that exist in the table and the
   * primary keys that they map to
   */
  private final Map<String, List<ForeignKey>> tableToForeignKeys;

  /**
   * Sets up a SchemaInferrer by reading the database's metadata.
   * @param conn
   */
  public SchemaInferrer(Connection conn) {
    columnToTable = new HashMap<>();
    tableToForeignKeys = new HashMap<>();
    tables = new ArrayList<>();

    try {
      DatabaseMetaData md = conn.getMetaData();

      // Find all the table names.
      tables.addAll(findColumnNames(md));
      for (String table : tables) {
        // Fill in columnToTable.
        populateColumnToTable(table, md);
        // Fill in table to foreign keys.
        populateTableToForeignKeys(table, md);
      }
    } catch (SQLException e) {
      log.info("Error generating the StarSchemaStore");
      e.printStackTrace();
    }
  }

  private List<String> findColumnNames(DatabaseMetaData md)
      throws SQLException {
    List<String> tables = new ArrayList<>();
    ResultSet rs = md.getTables(null, null, "%", null);
    while (rs.next()) {
      tables.add(rs.getString(3).toLowerCase());
    }
    return tables;
  }

  private void populateColumnToTable(String table, DatabaseMetaData md)
      throws SQLException {
    ResultSet columns = md.getColumns(null, null, table, null);
    while (columns.next()) {
      String col = columns.getString("COLUMN_NAME").toLowerCase();
      Set<String> previousSet =
          columnToTable.getOrDefault(col, new HashSet<String>());
      previousSet.add(table);
      columnToTable.put(col, previousSet);
    }
  }

  private void populateTableToForeignKeys(String table, DatabaseMetaData md)
      throws SQLException {
    ResultSet foreignKeys = md.getImportedKeys(null, null, table);
    while (foreignKeys.next()) {
      String fkTableName = foreignKeys.getString("FKTABLE_NAME").toLowerCase();
      String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME").toLowerCase();
      String pkTableName = foreignKeys.getString("PKTABLE_NAME").toLowerCase();
      String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME").toLowerCase();
      List<ForeignKey> previousList = tableToForeignKeys.getOrDefault(
          fkTableName, new ArrayList<ForeignKey>());
      previousList.add(
          new ForeignKey(fkColumnName, fkTableName, pkColumnName, pkTableName));
      tableToForeignKeys.put(fkTableName, previousList);
    }
  }

  /**
   * @param table The table that we want to aggregate over / select from.
   * @param column The column that were filtering on.
   * @return A JoinRecipe that can create the table to filter one and select
   *    from.
   */
  public JoinRecipe infer(
      String table, @Nullable String aggregation, List<String> comparisons) {

    // TODO: This only works for joining a fact table to a single dimension
    // table right now. It fails because every time we criteriaTables.retainAll
    // for one column, it kills whatever was important for the previous column.
    // The generic approach here would be more like, find the tables that work
    // for each column, then find the minimum subset of tables that works for
    // all columns.

    // Initially we can join with any table.
    Set<String> criteriaTables = new HashSet<>(tables);

    // Aggregate all the columns we are interested in.
    List<String> columns = new ArrayList<>(comparisons);
    columns.add(aggregation);
    columns.removeAll(Collections.singleton(null));

    // For each column we are interested, we need to filter down only tables
    // that contain it (ignoring columns that already exist in our base table).
    for (String column : columns) {
      Set<String> newCriteria =
          columnToTable.getOrDefault(column, new HashSet<String>());
      if (!newCriteria.contains(table)) {
        criteriaTables.retainAll(newCriteria);
      }
    }

    // If all the columns could be in our base table then we don't need to join.
    if (criteriaTables.contains(table)) {
      return new OneTableJoinRecipe(table);
    } else if (criteriaTables.size() > 0) {
      // If there are multiple choices, we choose the first one that connects
      // to our table.
      List<ForeignKey> foreignKeys =
          tableToForeignKeys.getOrDefault(table, new ArrayList<ForeignKey>());
      for (ForeignKey foreignKey : foreignKeys) {
        String destinationTable = foreignKey.getDestinationTable();
        if (criteriaTables.contains(destinationTable)) {
         return new TwoTableJoinRecipe(foreignKey,
             inferPrefixes(aggregation, comparisons, table, destinationTable));
        }
      }
    }
    return new InvalidJoinRecipe();
  }

  /**
   * @param column the column of interest
   * @param sourceTable the table of interest
   * @param destinationTable the table being joined
   * @return which table the column belongs to.
   */
  private String inferPrefix(
      @Nullable String column, String sourceTable, String destinationTable) {
    if (column == null) {
      return null;
    }
    Set<String> tables = columnToTable.get(column);
    return (tables.contains(sourceTable)) ? sourceTable : destinationTable;
  }

  private InferredContext inferPrefixes(
      @Nullable String aggregation,
      List<String> comparisons,
      String table,
      String destinationTable) {
    InferredContext ctx = new InferredContext();
    ctx.setAggregationPrefix(inferPrefix(aggregation, table, destinationTable));
    for (String comparison : comparisons) {
      ctx.addComparisonPrefix(inferPrefix(comparison, table, destinationTable));
    }
    return ctx;
  }

  public List<String> getTables() {
    return tables;
  }

  public Map<String, Set<String>> getColumnsToTable() {
    return columnToTable;
  }
}