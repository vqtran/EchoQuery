package echoquery.sql.model;

import java.util.Optional;

import javax.annotation.Nullable;

public class ColumnName {
  private Optional<String> table;
  private Optional<String> column;
  private ColumnType type;

  public ColumnName() {
    this.table = Optional.empty();
    this.column = Optional.empty();
    this.type = ColumnType.UNKNOWN;
  }

  public ColumnName(
      @Nullable String table, @Nullable String column, ColumnType type) {
    this.table = Optional.ofNullable(table);
    this.column = Optional.ofNullable(column);
    this.type = type;
  }

  public Optional<String> getTable() {
    return table;
  }

  public Optional<String> getColumn() {
    return column;
  }

  public ColumnType getType() {
    return type;
  }

  public ColumnName setTable(@Nullable String table) {
    this.table = Optional.ofNullable(table);
    return this;
  }

  public ColumnName setColumn(@Nullable String column) {
    this.column = Optional.ofNullable(column);
    return this;
  }

  public ColumnName setType(ColumnType type) {
    this.type = type;
    return this;
  }
}
