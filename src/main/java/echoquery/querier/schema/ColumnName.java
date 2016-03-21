package echoquery.querier.schema;

import java.io.Serializable;

import javax.annotation.Nullable;

import com.google.common.base.Optional;

public class ColumnName implements Serializable {
  /**
   * Generated for Serialization.
   */
  private static final long serialVersionUID = 6840946576757170260L;

  private Optional<String> table;
  private Optional<String> column;
  private ColumnType type;

  public ColumnName() {
    this.table = Optional.absent();
    this.column = Optional.absent();
    this.type = ColumnType.UNKNOWN;
  }

  public ColumnName(
      @Nullable String table, @Nullable String column, ColumnType type) {
    this.table = Optional.fromNullable(table);
    this.column = Optional.fromNullable(column);
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
    this.table = Optional.fromNullable(table);
    return this;
  }

  public ColumnName setColumn(@Nullable String column) {
    this.column = Optional.fromNullable(column);
    return this;
  }

  public ColumnName setType(ColumnType type) {
    this.type = type;
    return this;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((column == null) ? 0 : column.hashCode());
    result = prime * result + ((table == null) ? 0 : table.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ColumnName other = (ColumnName) obj;
    if (column == null) {
      if (other.getColumn() != null)
        return false;
    } else if (!column.equals(other.getColumn()))
      return false;
    if (table == null) {
      if (other.getTable() != null)
        return false;
    } else if (!table.equals(other.getTable()))
      return false;
    if (type != other.getType())
      return false;
    return true;
  }

}
