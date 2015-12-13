package echoquery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.slu.Slot;
import com.amazon.speech.speechlet.Session;

import static org.junit.Assert.assertEquals;
import echoquery.intents.AggregationHandler;
import echoquery.intents.NarrowHandler;
import echoquery.sql.QueryRequest;
import echoquery.utils.Serializer;
import echoquery.utils.SessionUtil;
import echoquery.utils.SlotUtil;

public class NarrowHandlerTest {
  private NarrowHandler handler =
      new NarrowHandler(TestConnection.getInstance(), new AggregationHandler(TestConnection.getInstance()));

  private Map<String, Slot> newNarrowEmptySlots() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, null);
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, null);
    addSlotValue(slots, SlotUtil.COMPARATOR_1, null);
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, null);
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER_1, null);
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, null);
    return slots;
  }

  private void addSlotValue(
      Map<String, Slot> slots, String slotName, String slotValue) {
    slots.put(slotName,
        Slot.builder().withName(slotName).withValue(slotValue).build());
  }
  
  private Session newSimpleSession() {
    Session session = Session.builder().withSessionId("1").build();
    Map<String, Slot> slots = AggregationHandlerTest.newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "how many");
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

  private Session newWhereSession() {
    Session session = Session.builder().withSessionId("1").build();
    Map<String, Slot> slots = AggregationHandlerTest.newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "how many");
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

  private Session newGroupedBySession() {
    Session session = Session.builder().withSessionId("1").build();
    Map<String, Slot> slots = AggregationHandlerTest.newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "how many");
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

  private void assertResponse(Map<String, Slot> slots, Session session, String expected) {
    assertEquals(expected,
        handler.getResponseInEnglish(Intent.builder()
            .withName("AggregationIntent").withSlots(slots).build(), session));
  }

  @Test
  public void testWhere() {
    Session session = this.newSimpleSession();
    
    Map<String, Slot> slots = newNarrowEmptySlots();
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "speakers");
    assertResponse(slots, session, "There are two rows in the sales table"
        + " where product is equal to speakers.");
  }

  @Test
  public void testGroupBy() {
    Session session = this.newSimpleSession();
    
    Map<String, Slot> slots = newNarrowEmptySlots();
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "product");
    assertResponse(slots, session, "There is one row in the sales table for "
        + "the product camera, two rows for speakers, and one row for tv.");
  }
  
  @Test
  public void testOverwriteGroupBy() {
    Session session = this.newGroupedBySession();
    
    Map<String, Slot> slots = newNarrowEmptySlots();
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "product");
    assertResponse(slots, session, "There is one row in the sales table for "
        + "the product camera, two rows for speakers, and one row for tv.");
  }

  @Test
  public void testWithWhere() {
    Session session = this.newWhereSession();

    Map<String, Slot> slots = newNarrowEmptySlots();
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "speakers");
    assertResponse(slots, session, "There is one row in the sales table where "
        + "store is not equal to Warwick and product is equal to speakers.");
  }
}