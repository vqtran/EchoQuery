package echoquery;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.slu.Slot;
import com.amazon.speech.speechlet.SpeechletResponse;

import echoquery.intents.AggregationHandler;
import echoquery.utils.SlotUtil;

public class AggregationHandlerTest {
  AggregationHandler handler = new AggregationHandler();

  private void addSlotValue(
      Map<String, Slot> slots, String slotName, String slotValue) {
    slots.put(slotName,
        Slot.builder().withName(slotName).withValue(slotValue).build());
  }

  private void assertResponse(Map<String, Slot> slots, String expected) {
    assertEquals(expected,
        handler.getResponseInEnglish(Intent.builder()
            .withName("AggregationIntent").withSlots(slots).build(), null));
  }

  @Test
  public void testCountWithoutWhere() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "how many");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, null);
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN, null);
    addSlotValue(slots, SlotUtil.COMPARATOR, null);
    addSlotValue(slots, SlotUtil.COLUMN_VALUE, null);
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER, null);
    assertResponse(slots, "There are four rows in the sales table.");
  }

  @Test
  public void testAverageWithoutWhere() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "average");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN, null);
    addSlotValue(slots, SlotUtil.COMPARATOR, null);
    addSlotValue(slots, SlotUtil.COLUMN_VALUE, null);
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER, null);
    assertResponse(slots, "The average of the count column in the sales table "
        + "is two point two five.");
  }

  @Test
  public void testSumWithoutWhere() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "total");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN, null);
    addSlotValue(slots, SlotUtil.COMPARATOR, null);
    addSlotValue(slots, SlotUtil.COLUMN_VALUE, null);
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER, null);
    assertResponse(slots, "The total of the count column in the sales table "
        + "is nine.");
  }

  @Test
  public void testMinWithoutWhere() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "minimum");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN, null);
    addSlotValue(slots, SlotUtil.COMPARATOR, null);
    addSlotValue(slots, SlotUtil.COLUMN_VALUE, null);
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER, null);
    assertResponse(slots, "The minimum value of the count column in the sales "
        + "table is one.");
  }

  @Test
  public void testMaxWithoutWhere() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "maximum");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN, null);
    addSlotValue(slots, SlotUtil.COMPARATOR, null);
    addSlotValue(slots, SlotUtil.COLUMN_VALUE, null);
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER, null);
    assertResponse(slots, "The maximum value of the count column in the sales "
        + "table is three.");
  }

  @Test
  public void testWithEnumeratedWhere() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "how many");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, null);
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE, "speakers");
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER, null);
    assertResponse(slots, "There are two rows in the sales table"
        + " where the value in the product column is equal to speakers.");
  }

  @Test
  public void testWithNumericalWhere() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "jobs");
    addSlotValue(slots, SlotUtil.AGGREGATE, "average");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "salary");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN, "salary");
    addSlotValue(slots, SlotUtil.COMPARATOR, "is greater than");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE, null);
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER, "15");
    assertResponse(slots, "The average of the salary column in the jobs table "
        + "where the value in the salary column is greater than fifteen is "
        + "two hundred sixty.");
  }

  @Test
  public void testRequiringJoinForAggregationColumn() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "employees");
    addSlotValue(slots, SlotUtil.AGGREGATE, "average");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "salary");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN, "name");
    addSlotValue(slots, SlotUtil.COMPARATOR, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE, "vinh");
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER, null);
    assertResponse(slots, "The average of the salary column in the employees "
        + "table where the value in the name column is not equal to vinh is "
        + "two hundred fifty five.");
  }

  @Test
  public void testRequiringJoinForComparisonColumn() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "employees");
    addSlotValue(slots, SlotUtil.AGGREGATE, "count");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, null);
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN, "title");
    addSlotValue(slots, SlotUtil.COMPARATOR, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE, "professor");
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER, null);
    assertResponse(slots, "There are two rows in the employees table where the "
        + "value in the title column is not equal to professor.");
  }

  @Test
  public void testRequiringJoinForBothAggregateAndComparisonColumns() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "employees");
    addSlotValue(slots, SlotUtil.AGGREGATE, "average");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "salary");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN, "title");
    addSlotValue(slots, SlotUtil.COMPARATOR, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE, "professor");
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER, null);
    assertResponse(slots, "The average of the salary column in the employees "
        + "table where the value in the title column is not equal to professor "
        + "is ten.");
  }
}
