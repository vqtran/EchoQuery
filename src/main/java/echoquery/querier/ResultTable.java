package echoquery.querier;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Frontend friendly representation of the resulting table from the user's SQL
 * query. Implemented as a utility wrapper around a columnar translation of
 * the ResultSet into a JSONObject.
 */

public class ResultTable {
  private ResultSetMetaData rsmd;
  private JSONObject data;

  public ResultTable(ResultSet rs) throws SQLException, JSONException {
    this.rsmd = rs.getMetaData();
    this.data = convert(rs);
  }

  public JSONObject json() {
    return data;
  }

  public int numRows() {
    if (data.keySet().isEmpty()) {
      return 0;
    }
    return data.getJSONArray(data.keys().next()).length();
  }

  public int numCols() {
    return data.keySet().size();
  }

  public double getDouble(String column, int row) {
    return data.getJSONArray(column).getDouble(row);
  }

  public String getString(String column, int row) {
    return data.getJSONArray(column).getString(row);
  }

  private JSONObject convert(ResultSet rs) throws SQLException, JSONException {
    int numColumns = rsmd.getColumnCount();

    JSONObject json = new JSONObject();
    for (int i = 1; i <= numColumns; i++) {
      json.put(getColumnWithTable(i, rsmd), Collections.emptyList());
    }

    rs.beforeFirst();
    while(rs.next()) {
      for (int i = 1; i <= numColumns; i++) {
        String columnName = getColumnWithTable(i, rsmd);
        switch (rsmd.getColumnType(i)) {
          case java.sql.Types.ARRAY:
            json.getJSONArray(columnName).put(rs.getArray(columnName));
            break;
          case java.sql.Types.BIGINT:
            json.getJSONArray(columnName).put(rs.getInt(columnName));
            break;
          case java.sql.Types.BOOLEAN:
            json.getJSONArray(columnName).put(rs.getBoolean(columnName));
            break;
          case java.sql.Types.BLOB:
            json.getJSONArray(columnName).put(rs.getBlob(columnName));
            break;
          case java.sql.Types.DOUBLE:
            json.getJSONArray(columnName).put(rs.getDouble(columnName));
            break;
          case java.sql.Types.FLOAT:
            json.getJSONArray(columnName).put(rs.getFloat(columnName));
            break;
          case java.sql.Types.INTEGER:
            json.getJSONArray(columnName).put(rs.getInt(columnName));
            break;
          case java.sql.Types.NVARCHAR:
            json.getJSONArray(columnName).put(rs.getNString(columnName));
            break;
          case java.sql.Types.VARCHAR:
            json.getJSONArray(columnName).put(rs.getString(columnName));
            break;
          case java.sql.Types.TINYINT:
            json.getJSONArray(columnName).put(rs.getInt(columnName));
            break;
          case java.sql.Types.SMALLINT:
            json.getJSONArray(columnName).put(rs.getInt(columnName));
            break;
          case java.sql.Types.DATE:
            json.getJSONArray(columnName).put(rs.getDate(columnName));
            break;
          case java.sql.Types.TIMESTAMP:
            json.getJSONArray(columnName).put(rs.getTimestamp(columnName));
            break;
          default:
            json.getJSONArray(columnName).put(rs.getObject(columnName));
        }
      }
    }
    return json;
  }

  private String getColumnWithTable(int i, ResultSetMetaData rsmd)
      throws SQLException {
    return (rsmd.getTableName(i).isEmpty()) ?
        rsmd.getColumnName(i)
        : String.join(".", rsmd.getTableName(i), rsmd.getColumnName(i));
  }
}
