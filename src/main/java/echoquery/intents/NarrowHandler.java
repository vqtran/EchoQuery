package echoquery.intents;

import java.io.IOException;
import java.sql.Connection;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;

import echoquery.sql.Querier;
import echoquery.sql.QueryRequest;
import echoquery.sql.QueryResult;
import echoquery.sql.model.ColumnType;
import echoquery.utils.Response;
import echoquery.utils.Serializer;
import echoquery.utils.SessionUtil;
import echoquery.utils.SlotUtil;

public class NarrowHandler implements IntentHandler {

  private static final Logger log =
      LoggerFactory.getLogger(NarrowHandler.class);
  private final Querier querier;

  public NarrowHandler(Connection conn) {
    this.querier = new Querier(conn);
  }

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    try {
      QueryRequest request = 
          updateRequest(intent, (QueryRequest) Serializer.deserialize(
              (String) session.getAttribute(SessionUtil.REQUEST_ATTRIBUTE)));
      session.setAttribute(
          SessionUtil.REQUEST_ATTRIBUTE, Serializer.serialize(request));

      // Execute the request and handle results.
      QueryResult result = querier.execute(request);
      if (result.getStatus() == QueryResult.Status.REPAIR_REQUEST) {
        return Response.ask(result.getMessage(), result.getMessage());
      } else {
        return Response.say(result.getMessage());
      }
    } catch (ClassNotFoundException | IOException e) {
      e.printStackTrace();
      return Response.unexpectedError();
    }
  }
    
  /**
   * Exposed for testing purposes - SpeechletResponse is impossible to inspect.
   */
  public String getResponseInEnglish(Intent intent, Session session) {
    QueryRequest previousRequest;
    try {
      previousRequest = (QueryRequest) Serializer.deserialize(
                (String) session.getAttribute(SessionUtil.REQUEST_ATTRIBUTE));
      return querier.execute(updateRequest(intent, previousRequest)).getMessage();
    } catch (ClassNotFoundException | IOException e) {
      e.printStackTrace();
      return "There was an error generating the response in English";
    }
  }
  
  QueryRequest updateRequest(Intent intent, QueryRequest previousRequest) {
    return previousRequest
        .addWhereClause(SlotUtil.parseColumnSlot(
            intent.getSlot(SlotUtil.COMPARISON_COLUMN_1).getValue())
                .setType((intent.getSlot(
                    SlotUtil.COLUMN_NUMBER_1).getValue() != null)
                ? ColumnType.NUMBER : ColumnType.STRING),
            intent.getSlot(SlotUtil.COMPARATOR_1).getValue(),
            ObjectUtils.defaultIfNull(
                intent.getSlot(SlotUtil.COLUMN_VALUE_1).getValue(),
                intent.getSlot(SlotUtil.COLUMN_NUMBER_1).getValue()))
        .setGroupByColumn(SlotUtil.parseColumnSlot(
            intent.getSlot(SlotUtil.GROUP_BY_COLUMN).getValue()));
  }
}
