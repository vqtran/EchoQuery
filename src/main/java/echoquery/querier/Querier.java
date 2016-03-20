package echoquery.querier;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.sql.tree.Query;

import echoquery.querier.QueryResult.Status;
import echoquery.querier.infer.SchemaInferrer;
import echoquery.querier.translate.RequestTranslator;
import echoquery.querier.translate.ResultTranslator;
import echoquery.utils.SlotUtil;

/**
 * The Querier class takes in an unbuilt QueryRequest object, validates that its
 * contents are well-formed, builds, then executes the query.
 */
public class Querier {

  private static final Logger log = LoggerFactory.getLogger(Querier.class);
  private Connection conn;
  private SchemaInferrer inferrer;
  private RequestTranslator queryTranslator;
  private ResultTranslator resultTranslator;

  public Querier(Connection conn) {
    this.conn = conn;
    this.inferrer = new SchemaInferrer(conn);
    this.queryTranslator = new RequestTranslator(inferrer);
    this.resultTranslator = new ResultTranslator(inferrer);
  }

  /**
   * Validate then execute a QueryRequest.
   * @param request
   * @return A QueryResult with either a success or failure status and message.
   */
  public QueryResult execute(QueryRequest request) {
    // First validate the request, interactively mending malformed requests.
    Optional<QueryResult> invalidation = validate(request);
    if (invalidation.isPresent()) {
      return invalidation.get();
    }
    // If it gets here the request had all necessary fields, so now try to
    // form a translation into SQL, adding inferred joins if necessary.
    Query ast;
    String sql;
    try {
      ast = queryTranslator.buildAST(request);
      sql = queryTranslator.translate(request);
    } catch (QueryBuildException e) {
      return e.getResult();
    }
    // Finally, execute the SQL query.
    try {
      Statement statement = conn.createStatement();
      ResultTable result = new ResultTable(statement.executeQuery(sql));
      return new QueryResult(
          QueryResult.Status.SUCCESS,
          resultTranslator.translate(result, request, ast),
          result.json());
    } catch (SQLException e) {
      log.error(e.getMessage());
      return new QueryResult(Status.FAILURE,
          "There was a problem with the execution of your query itself, "
          + "perhaps a table or column was referenced that does not exist. "
          + "Please try again. ");
    }
  }

  /**
   * Validate that the unbuilt fields in the QueryRequest are well formed to
   * make sure no null pointer exceptions occur during the building process.
   * @param request
   * @return Optional.empty() if seems to be a valid QueryRequest.
   *    A non-successful Optional[QueryResult] otherwise.
   */
  public Optional<QueryResult> validate(QueryRequest request) {
    // Table name is present.
    if (!request.getFromTable().isPresent()) {
      return Optional.of(new QueryResult(Status.FAILURE,
          "I'm not sure what table you're interested in, or maybe you referred "
          + "to one that doesn't exist? Please try again."));
    }

    String table = request.getFromTable().get();

    // Table name exists in the database.
    if (!inferrer.getTables().contains(table)) {
      return Optional.of(new QueryResult(Status.FAILURE,
          "The table you requested, " + table + ", doesn't seem to exist in the"
          + " database. Please try another table name."));
    }

    // Function is present.
    if (!request.getSelectAll().isPresent()
        && !request.getAggregationFunc().isPresent()) {
      return Optional.of(new QueryResult(Status.FAILURE,
          "I can't tell if you want to know the rows, the number of rows or "
          + "the average, sum, min, or max of a particular column in the "
          + table + " table. Please try again."));
    }

    // Non-get and non-count aggregation functions require a column to
    // aggregate over.
    if (!request.getSelectAll().isPresent()
        && request.getAggregationFunc().isPresent()
        && !request.getAggregationFunc().get().equals("COUNT")
        && !request.getAggregationColumn().getColumn().isPresent()) {
      return Optional.of(new QueryResult(Status.FAILURE,
          "I'm not sure what column you want me to find the "
          + SlotUtil.aggregationFunctionToEnglish(
              request.getAggregationFunc().get())
          + " over from the " + table
          + " table. Please try again."));
    }

    // Aggregation column, if present, exists in the database.
    if (request.getAggregationColumn().getColumn().isPresent()) {
      String aggregationColumn =
          request.getAggregationColumn().getColumn().get();
      if (!inferrer.getColumnsToTable().containsKey(aggregationColumn)) {
        return Optional.of(new QueryResult(Status.FAILURE,
            "The " + request.getAggregationColumn().getColumn().get()
            + " column that you've asked me to find the "
            + SlotUtil.aggregationFunctionToEnglish(
                request.getAggregationFunc().get())
            + " of doesn't seem to exist in the database. Please try another"
            + " column name."));
      }
    }

    String[] ordinals = {"first", "second", "third"};
    // Valid where for each clause.
    for (int i = 0; i < request.getComparisonColumns().size(); i++) {
      // The column to compare is present.
      if (!request.getComparisonColumns().get(i).getColumn().isPresent()) {
        return Optional.of(new QueryResult(Status.FAILURE,
            "I can't tell what column you're interested in filtering on in "
            + "your " + ordinals[i] + " where clause. Please try again."));
      }

      String comparisonColumn =
          request.getComparisonColumns().get(i).getColumn().get();

      // The column to compare exists in the database.
      if (!inferrer.getColumnsToTable().containsKey(comparisonColumn)) {
        return Optional.of(new QueryResult(Status.FAILURE,
            "The " + comparisonColumn + " column that you are referring to in "
            + "the " + ordinals[i] + " where clause doesn't seem to exist in "
            + "the database. Please try another column name."));
      }

      // The comparison operator to use is present.
      if (!request.getComparators().get(i).isPresent()) {
        return Optional.of(new QueryResult(Status.FAILURE,
            "I can't tell if you are looking for values equal to, greater than,"
            + " or less than the specified value in the "
            + request.getComparisonColumns().get(i).getColumn().get()
            + " column in your "
            + ordinals[i] + " where clause. Please try again."));
      }

      // The value to compare the column to is present.
      if (!request.getComparisonValues().get(i).isPresent()) {
        return Optional.of(new QueryResult(Status.FAILURE,
            "I can't tell if in your " + ordinals[i] + " where clause, you are "
            + "looking for values in the "
            + request.getComparisonColumns().get(i).getColumn().get()
            + " column"
            + SlotUtil.comparisonTypeToEnglish(
                request.getComparators().get(i).get())
            + "what value. Please try again."));
      }

      // If there exists a where clause after this one, there must be a
      // corresponding AND or OR operator between them.
      if (i < request.getComparisonColumns().size() - 1
          && (i >= request.getComparisonBinaryOperators().size()
              || !request.getComparisonBinaryOperators().get(i).isPresent())) {
        return Optional.of(new QueryResult(Status.FAILURE,
            "It seems like you have more than one where condition but no and "
            + "or or connecting them, and I'm not sure what to do with this."
            + " Please try again."));
      }
    }

    // Group by column exists in the database.
    if (request.getGroupByColumn().getColumn().isPresent()
        && !inferrer.getColumnsToTable().containsKey(
            request.getGroupByColumn().getColumn().get())) {
      return Optional.of(new QueryResult(Status.FAILURE,
          "The " + request.getGroupByColumn().getColumn().get()
          + " column that you are referring to in your group by doesn't seem "
          + "to exist in the database. Please try again."));
    }

    return Optional.empty();
  }
}
