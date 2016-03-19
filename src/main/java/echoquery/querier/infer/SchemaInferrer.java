package echoquery.querier.infer;

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

import echoquery.querier.schema.ColumnName;
import echoquery.querier.schema.ForeignKey;

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

  public List<String> getTables() {
    return tables;
  }

  public Map<String, Set<String>> getColumnsToTable() {
    return columnToTable;
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
   * @param baseTable The table that we want to aggregate over / select from.
   * @param column The column that were filtering on.
   * @return A JoinRecipe that can create the table to filter one and select
   *    from.
   */
  public JoinRecipe infer(
      String baseTable,
      ColumnName aggregation,
      List<ColumnName> comparisons,
      ColumnName groupBy) {

    InferredContext inference = new InferredContext();

    // If there exists an aggregation column to infer on.
    if (aggregation.getColumn().isPresent()) {
      inference.setAggregationPrefix(inferTable(baseTable, aggregation));
    }

    // Infer for each column in the where clause now.
    for (ColumnName column : comparisons) {
      inference.addComparisonPrefix(inferTable(baseTable, column));
    }

    // If there exists a group-by column to infer on.
    if (groupBy.getColumn().isPresent()) {
      inference.setGroupBy(inferTable(baseTable, groupBy));
    }

    // Validate that we've been able to assign a table to every column.
    // If there were ambiguities we prompt the user.

    if (aggregation.getColumn().isPresent()
        && !inference.getAggregationPrefix().isPresent()) {
      return InvalidJoinRecipe.ambiguousTableForColumn(
          columnToTable.get(aggregation.getColumn().get()), inference);
    }

    for (int i = 0; i < comparisons.size(); i++) {
      if (!inference.getComparisons().get(i).isPresent()) {
        return InvalidJoinRecipe.ambiguousTableForColumn(
            columnToTable.get(comparisons.get(i).getColumn().get()), inference);
      }
    }

    if (groupBy.getColumn().isPresent()
        && !inference.getGroupByPrefix().isPresent()) {
      return InvalidJoinRecipe.ambiguousTableForColumn(
          columnToTable.get(groupBy.getColumn().get()), inference);
    }

    // Now get all the distinct dimension tables we need to join on to satisfy
    // these columns. This is all inferred tables except for our base table.
    Set<String> joinTables = inference.distinctTables();
    joinTables.remove(baseTable);

    // Get all the foreign keys for our base table.
    List<ForeignKey> foreignKeys =
        tableToForeignKeys.getOrDefault(baseTable, new ArrayList<ForeignKey>());

    // Find the ones relevant to the tables we wanted to join on.
    Set<ForeignKey> relevantKeys = new HashSet<>();
    for (ForeignKey key : foreignKeys) {
      // Whenever we find a relevant one we remove it from joinTables.
      if (joinTables.remove(key.getDestinationTable())) {
        relevantKeys.add(key);
      }
    }

    // So if there was a foreign key leading out from our base table for each
    // table we wanted to join on, we then expect joinTables to be empty.
    if (!joinTables.isEmpty()) {
      return InvalidJoinRecipe.missingForeignKey(joinTables.iterator().next());
    }

    // We now know all the right foreign keys to join with!
    return new MultiTableJoinRecipe(baseTable, relevantKeys, inference);
  }

  /**
   * Infer from a ColumnName what table it belongs to.
   * @param column
   * @return
   */
  private String inferTable(String baseTable, ColumnName column) {
    // If the user specified and we don't need to infer its table, then don't.
    if (column.getTable().isPresent()) {
      return column.getTable().get();
    } else {
      // Otherwise, find the tables it could belong to.
      Set<String> candidateTables = columnToTable.getOrDefault(
          column.getColumn().get(), new HashSet<>());
      // If there's only one, this is it!
      if (candidateTables.size() == 1) {
        return candidateTables.iterator().next();
      }
      // If one of these is the base table, then we can default to the base
      // table.
      if (candidateTables.contains(baseTable)) {
        return baseTable;
      }
      // If there's multiple perfectly valid ones, we can't decide. Let the
      // prefix be null, we'll validate at the end.
      return null;
    }
  }
}