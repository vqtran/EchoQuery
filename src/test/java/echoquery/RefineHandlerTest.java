package echoquery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.slu.Slot;
import com.amazon.speech.speechlet.Session;

import static org.junit.Assert.assertEquals;
import echoquery.intents.QueryHandler;
import echoquery.querier.QueryRequest;
import echoquery.intents.RefineHandler;
import echoquery.utils.Serializer;
import echoquery.utils.SessionUtil;
import echoquery.utils.SlotUtil;

public class RefineHandlerTest {
  private RefineHandler handler = new RefineHandler(
      TestConnection.getInstance(),
      new QueryHandler(TestConnection.getInstance()));

  private static Map<String, Slot> newRefineEmptySlots() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.REFINE_TYPE, null);
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, null);
    addSlotValue(slots, SlotUtil.COMPARATOR_1, null);
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, null);
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER_1, null);
    addSlotValue(slots, SlotUtil.BINARY_LOGIC_OP_1, null);
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_2, null);
    addSlotValue(slots, SlotUtil.COMPARATOR_2, null);
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_2, null);
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER_2, null);
    addSlotValue(slots, SlotUtil.BINARY_LOGIC_OP_2, null);
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_3, null);
    addSlotValue(slots, SlotUtil.COMPARATOR_3, null);
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_3, null);
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER_3, null);
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, null);
    return slots;
  }

  private static void addSlotValue(
      Map<String, Slot> slots, String slotName, String slotValue) {
    slots.put(slotName,
        Slot.builder().withName(slotName).withValue(slotValue).build());
  }

  private static Session newSimpleSession() {
    Session session = Session.builder().withSessionId("1").build();
    Map<String, Slot> slots = QueryHandlerTest.newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.FUNC, "how many");
    QueryRequest request = QueryRequest.of(Intent.builder()
        .withName("AggregationIntent").withSlots(slots).build());

    try {
      session.setAttribute(
            SessionUtil.REQUEST_ATTRIBUTE, Serializer.serialize(request));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return session;
  }

  private static Session newWhereSession() {
    Session session = Session.builder().withSessionId("1").build();
    Map<String, Slot> slots = QueryHandlerTest.newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.FUNC, "how many");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "store");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "Warwick");
    QueryRequest request = QueryRequest.of(Intent.builder()
        .withName("AggregationIntent").withSlots(slots).build());

    try {
      session.setAttribute(
            SessionUtil.REQUEST_ATTRIBUTE, Serializer.serialize(request));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return session;
  }

  private static Session newGroupedBySession() {
    Session session = Session.builder().withSessionId("1").build();
    Map<String, Slot> slots = QueryHandlerTest.newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.FUNC, "how many");
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "store");
    QueryRequest request = QueryRequest.of(Intent.builder()
        .withName("AggregationIntent").withSlots(slots).build());

    try {
      session.setAttribute(
            SessionUtil.REQUEST_ATTRIBUTE, Serializer.serialize(request));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return session;
  }

  private void assertResponse(
      Map<String, Slot> slots, Session session, String expected) {
    assertEquals(expected,
        handler.getResponseInEnglish(Intent.builder()
            .withName("AggregationIntent").withSlots(slots).build(), session));
  }

  @Test
  public void testFirstWhere() {
    Session session = newSimpleSession();
    Map<String, Slot> slots = newRefineEmptySlots();
    addSlotValue(slots, SlotUtil.REFINE_TYPE, "and where");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "speakers");
    assertResponse(slots, session, "There are two rows in the sales table"
        + " where product is equal to speakers.");
  }

  @Test
  public void testAddWhere() {
    Session session = newWhereSession();
    Map<String, Slot> slots = newRefineEmptySlots();
    addSlotValue(slots, SlotUtil.REFINE_TYPE, "and where");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "speakers");
    assertResponse(slots, session, "There is one row in the sales table where "
        + "store is not equal to Warwick and product is equal to speakers.");
  }

  @Test
  public void testReplaceWhereMatch() {
    Session session = newWhereSession();
    Map<String, Slot> slots = newRefineEmptySlots();
    addSlotValue(slots, SlotUtil.REFINE_TYPE, "instead where");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "store");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "providence");
    assertResponse(slots, session, "There are two rows in the sales table "
        + "where store is equal to providence.");
  }

  @Test
  public void testReplaceWhereNoMatch() {
    Session session = newWhereSession();
    Map<String, Slot> slots = newRefineEmptySlots();
    addSlotValue(slots, SlotUtil.REFINE_TYPE, "instead where");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "speakers");
    assertResponse(slots, session, "There are two rows in the sales table "
        + "where product is equal to speakers.");
  }

  @Test
  public void testDropWhere() {
    Session session = newWhereSession();
    Map<String, Slot> slots = newRefineEmptySlots();
    addSlotValue(slots, SlotUtil.REFINE_TYPE, "remove where");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "store");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "warwick");
    assertResponse(slots, session, "There are four rows in the sales table.");
  }

  @Test
  public void testAddGroupBy() {
    Session session = newSimpleSession();
    Map<String, Slot> slots = newRefineEmptySlots();
    addSlotValue(slots, SlotUtil.REFINE_TYPE, "and");
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "product");
    assertResponse(slots, session, "There is one row in the sales table for "
        + "the product camera, two rows for speakers, and one row for tv.");
  }

  @Test
  public void testReplaceGroupBy() {
    Session session = newGroupedBySession();
    Map<String, Slot> slots = newRefineEmptySlots();
    addSlotValue(slots, SlotUtil.REFINE_TYPE, "instead");
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "product");
    assertResponse(slots, session, "There is one row in the sales table for "
        + "the product camera, two rows for speakers, and one row for tv.");
  }

  @Test
  public void testDropGroupBy() {
    Session session = newSimpleSession();
    Map<String, Slot> slots = newRefineEmptySlots();
    addSlotValue(slots, SlotUtil.REFINE_TYPE, "drop");
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "store");
    assertResponse(slots, session, "There are four rows in the sales table.");
  }

  @Test
  public void testWhereThenGroupBy() {
    Session session = newWhereSession();
    Map<String, Slot> slots = newRefineEmptySlots();
    addSlotValue(slots, SlotUtil.REFINE_TYPE, "and");
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "product");
    assertResponse(slots, session, "There is one row in the sales table where "
        + "store is not equal to Warwick for the product camera, one row for "
        + "speakers, and one row for tv.");
  }

  @Test
  public void testGroupByThenWhere() {
    Session session = newGroupedBySession();
    Map<String, Slot> slots = newRefineEmptySlots();
    addSlotValue(slots, SlotUtil.REFINE_TYPE, "and where");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "speakers");
    assertResponse(slots, session, "There is one row in the sales table where "
        + "product is equal to speakers for the store providence, and one row "
        + "for warwick.");
  }
}