package echoquery.intents;

import java.sql.Connection;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;

import echoquery.sql.Querier;
import echoquery.sql.QueryRequest;
import echoquery.sql.model.ColumnType;
import echoquery.utils.Response;
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
    return Response.say(getResponseInEnglish(intent, session));
  }

  /**
   * Exposed for testing purposes - SpeechletResponse is impossible to inspect.
   */
  public String getResponseInEnglish(Intent intent, Session session) {
    QueryRequest previousRequest = (QueryRequest) session.getAttribute(SessionUtil.REQUEST_ATTRIBUTE);
    return querier.execute(updateRequest(intent, previousRequest)).getMessage();
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
