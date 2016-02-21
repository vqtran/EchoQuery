package echoquery.utils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Converts a JDBC ResultSet into a columnar format in JSON.
 */

public final class ResultSetConverter {

  private ResultSetConverter() {}

  public static JSONObject convert(ResultSet rs)
      throws SQLException, JSONException {
    ResultSetMetaData rsmd = rs.getMetaData();
    int numColumns = rsmd.getColumnCount();

    JSONObject json = new JSONObject();
    for (int i = 1; i <= numColumns; i++) {
      String columnName = rsmd.getTableName(i) + "." + rsmd.getColumnName(i);
      json.put(columnName, Collections.emptyList());
    }

    rs.beforeFirst();
    while(rs.next()) {
      for (int i = 1; i <= numColumns; i++) {
        String columnName = rsmd.getTableName(i) + "." + rsmd.getColumnName(i);
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
}
