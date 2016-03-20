package echoquery.intents;

import java.io.IOException;
import java.sql.Connection;

import org.apache.commons.lang3.ObjectUtils;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;

import echoquery.querier.QueryRequest;
import echoquery.querier.infer.InferredContext;
import echoquery.querier.infer.JoinRecipe;
import echoquery.querier.infer.SchemaInferrer;
import echoquery.utils.Response;
import echoquery.utils.Serializer;
import echoquery.utils.SessionUtil;
import echoquery.utils.SlotUtil;

/**
 * Handles when users clarify what table a column belonged to. Mends the
 * previous request with what the user clarified, then re-runs it through
 * AggregationIntent.
 */
public class ClarifyHandler implements IntentHandler {

  private final QueryHandler aggregationHandler;
  private final SchemaInferrer inferrer;

  public ClarifyHandler(
      Connection conn, QueryHandler aggregationHandler) {
    this.aggregationHandler = aggregationHandler;
    this.inferrer = new SchemaInferrer(conn);
  }

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    // Get the previously tried but failed request.
    QueryRequest request;
    try {
      request = (QueryRequest) Serializer.deserialize(
          (String) session.getAttribute(SessionUtil.REQUEST_ATTRIBUTE));
    } catch (ClassNotFoundException | IOException e) {
      e.printStackTrace();
      return Response.unexpectedError(session);
    }

    // Get the clarified values from the slots. It could either be clarifed
    // as just a table or table and column.
    String tableName = intent.getSlot(SlotUtil.TABLE_NAME).getValue();
    String tableAndColumn =
        intent.getSlot(SlotUtil.TABLE_AND_COLUMN).getValue();

    // Parse out just the table if its table and column.
    String table = ObjectUtils.defaultIfNull(tableName, tableAndColumn);
    if (tableName == null) {
      table = SlotUtil.parseColumnSlot(table).getTable().orNull();
    }

    // Generate the InvalidJoinRecipe again, to find where the first ambiguity
    // was.
    JoinRecipe from = inferrer.infer(
        request.getFromTable().get(),
        request.getAggregationColumn(),
        request.getComparisonColumns(),
        request.getGroupByColumn());

    if (!from.isValid()) {
      // Go through and find the first ambiguity, setting it's table name to
      // what was clarified and stopping after fixing that first one.
      // Order here is important: Aggregation, where clauses, group by.
      InferredContext inference = from.getContext();
      boolean foundFix = false;

      // Was it the aggregation column?
      if (request.getAggregationColumn().getColumn().isPresent()
          && !inference.getAggregationPrefix().isPresent()) {
        request.getAggregationColumn().setTable(table);
        foundFix = true;
      } else {
        // It was any of the where columns in order?
        for (int i = 0; i < request.getComparisonColumns().size(); i++) {
          if (!inference.getComparisons().get(i).isPresent()) {
            request.getComparisonColumns().get(i).setTable(table);
            foundFix = true;
            break;
          }
        }
      }

      // if it wasn't the aggregation or where columns, is it the group by
      // column?
      if (!foundFix && request.getGroupByColumn().getColumn().isPresent()
          && !inference.getGroupByPrefix().isPresent()) {
        request.getGroupByColumn().setTable(table);
      }
    }

    // Run this mended request through the execution pipeline again.
    return aggregationHandler.respond(request, session);
  }
}
