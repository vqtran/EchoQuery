package echoquery.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import echoquery.sql.QueryResult.Status;
import echoquery.sql.formatter.SqlFormatter;
import echoquery.utils.SlotUtil;

public class Querier {

  private static final Logger log = LoggerFactory.getLogger(Querier.class);
  private Connection conn;
  private SchemaInferrer inferrer;

  public Querier(Connection conn) {
    this.conn = conn;
    this.inferrer = new SchemaInferrer(conn);
  }

  public QueryResult execute(QueryRequest request) {
    // First validate the request, interactively mending malformed requests.
    Optional<QueryResult> invalidation = validate(request);
    if (invalidation.isPresent()) {
      return invalidation.get();
    }
    // If it gets here the request had all necessary fields, so now try to
    // execute the SQL translation.
    String sql =
        SqlFormatter.formatSql(request.buildQuery(inferrer).getQuery());
    try {
      Statement statement = conn.createStatement();
      ResultSet result = statement.executeQuery(sql);
      return QueryResult.of(request, result);
    } catch (SQLException e) {
      log.error(e.getMessage());
      return new QueryResult(Status.FAILURE,
          "There was a problem with the execution of your query itself, "
          + "perhaps a table or column was referenced that does not exist. "
          + "Please try again. ");
    }
  }

  public Optional<QueryResult> validate(QueryRequest request) {
    if (!request.getFromTable().isPresent()) {
      return Optional.of(new QueryResult(Status.FAILURE,
          "I'm not sure what table you're interested in, or maybe you referred "
          + "to one that doesn't exist? Please try again."));
    }

    if (!request.getAggregationFunc().isPresent()) {
      return Optional.of(new QueryResult(Status.FAILURE,
          "I can't tell if you want to know the number of rows or the average, "
          + "sum, min, or max of a particular column in the "
          + request.getFromTable().get() + " table. Please try again."));
    }

    if (!request.getAggregationFunc().get().equals("COUNT")
        && !request.getAggregationColumn().isPresent()) {
      return Optional.of(new QueryResult(Status.FAILURE,
          "I'm not sure what column you want me to find the "
          + SlotUtil.aggregationFunctionToEnglish(
              request.getAggregationFunc().get())
          + " over from the " + request.getFromTable().get()
          + " table. Please try again."));
    }

    for (int i = 0; i < request.getComparisonColumns().size(); i++) {
      String index = "";
      switch (i) {
        case 0:
          index = "first";
          break;
        case 1:
          index = "second";
          break;
        case 2:
          index = "third";
          break;
      }

      if (!request.getComparisonColumns().get(i).isPresent()) {
        return Optional.of(new QueryResult(Status.FAILURE,
            "I can't tell what column you're interested in filtering on in "
            + "your " + index + " where clause. Please try again."));
      }
      if (!request.getComparators().get(i).isPresent()) {
        return Optional.of(new QueryResult(Status.FAILURE,
            "I can't tell if you are looking for values equal to, greater than,"
            + " or less than the specified value in the "
            + request.getComparisonColumns().get(i).get() + " column in your "
            + index + " where clause. Please try again."));
      }
      if (!request.getComparisonValues().get(i).isPresent()) {
        return Optional.of(new QueryResult(Status.FAILURE,
            "I can't tell if in your " + index + " where clause, you are "
            + "looking for values in the "
            + request.getComparisonColumns().get(i).get() + " column"
            + SlotUtil.comparisonTypeToEnglish(
                request.getComparators().get(i).get())
            + "what value. Please try again."));
      }
      if (i < request.getComparisonColumns().size() - 1
          && (i >= request.getComparisonBinaryOperators().size()
              || !request.getComparisonBinaryOperators().get(i).isPresent())) {
        return Optional.of(new QueryResult(Status.FAILURE,
            "It seems like you have more than one where condition but no and "
            + "or or connecting them, and I'm not sure what to do with this."
            + " Please try again."));
      }
    }

    return Optional.empty();
  }
}
