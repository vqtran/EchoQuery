package echoquery;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.slu.Slot;
import com.amazon.speech.speechlet.Session;

import echoquery.intents.AggregationHandler;
import echoquery.utils.SlotUtil;

public class AggregationHandlerTest {
  AggregationHandler handler =
      new AggregationHandler(TestConnection.getInstance());

  private Map<String, Slot> newEmptySlots() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, null);
    addSlotValue(slots, SlotUtil.AGGREGATE, null);
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, null);
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

  private void addSlotValue(
      Map<String, Slot> slots, String slotName, String slotValue) {
    slots.put(slotName,
        Slot.builder().withName(slotName).withValue(slotValue).build());
  }

  private void assertResponse(Map<String, Slot> slots, String expected) {
    assertEquals(expected,
        handler.getResponseInEnglish(Intent.builder()
            .withName("AggregationIntent").withSlots(slots).build(), Session.builder().withSessionId("1").build()));
  }

  @Test
  public void testCountWithoutWhere() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "how many");
    assertResponse(slots, "There are four rows in the sales table.");
  }

  @Test
  public void testAverageWithoutWhere() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "average");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    assertResponse(slots, "The average of the count column in the sales table "
        + "is two point two five.");
  }

  @Test
  public void testSumWithoutWhere() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "total");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    assertResponse(slots, "The total of the count column in the sales table "
        + "is nine.");
  }

  @Test
  public void testMinWithoutWhere() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "minimum");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    assertResponse(slots, "The minimum value of the count column in the sales "
        + "table is one.");
  }

  @Test
  public void testMaxWithoutWhere() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "maximum");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    assertResponse(slots, "The maximum value of the count column in the sales "
        + "table is three.");
  }

  @Test
  public void testWithEnumeratedWhere() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "how many");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "speakers");
    assertResponse(slots, "There are two rows in the sales table"
        + " where the value in the product column is equal to speakers.");
  }

  @Test
  public void testWithNumericalWhere() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "jobs");
    addSlotValue(slots, SlotUtil.AGGREGATE, "average");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "salary");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "salary");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is greater than");
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER_1, "15");
    assertResponse(slots, "The average of the salary column in the jobs table "
        + "where the value in the salary column is greater than fifteen is "
        + "two hundred sixty.");
  }

  @Test
  public void testWithTwoWhereClauses() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "how many");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "speakers");
    addSlotValue(slots, SlotUtil.BINARY_LOGIC_OP_1, "and");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_2, "store");
    addSlotValue(slots, SlotUtil.COMPARATOR_2, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_2, "warwick");
    assertResponse(slots, "There is one row in the sales table"
        + " where the value in the product column is equal to speakers and"
        + " the store column is not equal to warwick.");
  }

  @Test
  public void testWithThreeWhereClauses() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "how many");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "speakers");
    addSlotValue(slots, SlotUtil.BINARY_LOGIC_OP_1, "and");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_2, "store");
    addSlotValue(slots, SlotUtil.COMPARATOR_2, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_2, "warwick");
    addSlotValue(slots, SlotUtil.BINARY_LOGIC_OP_2, "and");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_3, "count");
    addSlotValue(slots, SlotUtil.COMPARATOR_3, "greater than");
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER_3, "5");
    assertResponse(slots, "There are zero rows in the sales table"
        + " where the value in the product column is equal to speakers and"
        + " the store column is not equal to warwick and the count column is"
        + " greater than five.");
  }

  @Test
  public void testWithThreeWhereClausesWithOrderOfOperations() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "average");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "speakers");
    addSlotValue(slots, SlotUtil.BINARY_LOGIC_OP_1, "or");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_2, "store");
    addSlotValue(slots, SlotUtil.COMPARATOR_2, "is equal to");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_2, "pawtucket");
    addSlotValue(slots, SlotUtil.BINARY_LOGIC_OP_2, "and");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_3, "product");
    addSlotValue(slots, SlotUtil.COMPARATOR_3, "is equal to");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_3, "camera");
    assertResponse(slots, "The average of the count column in the sales table"
        + " where the value in the product column is equal to speakers or"
        + " the store column is equal to pawtucket and the product column is"
        + " equal to camera is two point three three three.");
  }

  @Test
  public void testRequiringJoinForAggregationColumn() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "employees");
    addSlotValue(slots, SlotUtil.AGGREGATE, "average");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "salary");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "name");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "vinh");
    assertResponse(slots, "The average of the salary column in the employees "
        + "table where the value in the name column is not equal to vinh is "
        + "two hundred fifty five.");
  }

  @Test
  public void testRequiringJoinForFirstComparisonColumn() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "employees");
    addSlotValue(slots, SlotUtil.AGGREGATE, "count");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "title");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "professor");
    assertResponse(slots, "There are two rows in the employees table where the "
        + "value in the title column is not equal to professor.");
  }

  @Test
  public void testRequiringJoinForSecondComparisonColumn() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "employees");
    addSlotValue(slots, SlotUtil.AGGREGATE, "count");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "name");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "vinh");
    addSlotValue(slots, SlotUtil.BINARY_LOGIC_OP_1, "and");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_2, "title");
    addSlotValue(slots, SlotUtil.COMPARATOR_2, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_2, "professor");
    assertResponse(slots, "There is one row in the employees table where the "
        + "value in the name column is not equal to vinh and the title column "
        + "is not equal to professor.");
  }

  @Test
  public void testRequiringJoinForThirdComparisonColumn() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "employees");
    addSlotValue(slots, SlotUtil.AGGREGATE, "count");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "name");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "vinh");
    addSlotValue(slots, SlotUtil.BINARY_LOGIC_OP_1, "and");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_2, "title");
    addSlotValue(slots, SlotUtil.COMPARATOR_2, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_2, "professor");
    addSlotValue(slots, SlotUtil.BINARY_LOGIC_OP_2, "and");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_3, "salary");
    addSlotValue(slots, SlotUtil.COMPARATOR_3, "is greater than");
    addSlotValue(slots, SlotUtil.COLUMN_NUMBER_3, "100");
    assertResponse(slots, "There is one row in the employees table where the "
        + "value in the name column is not equal to vinh and the title column "
        + "is equal to professor and the salary column is greater than one "
        + "hundred.");
  }

  @Test
  public void testRequiringJoinForBothAggregateAndComparisonColumns() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "employees");
    addSlotValue(slots, SlotUtil.AGGREGATE, "average");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "salary");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "title");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "professor");
    assertResponse(slots, "The average of the salary column in the employees "
        + "table where the value in the title column is not equal to professor "
        + "is ten.");
  }

  @Test
  public void testCountWithGroupBy() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "how many");
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "store");
    assertResponse(slots, "There is one row in the sales table for the store "
        + "pawtucket, two rows for providence, and one row for warwick.");
  }

  @Test
  public void testAverageWithGroupBy() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "average");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "product");
    assertResponse(slots, "The average of the count column in the sales table "
        + "is three for the product camera, two for speakers, and two for tv.");
  }

  @Test
  public void testSumWithGroupBy() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "total");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "product");
    assertResponse(slots, "The total of the count column in the sales table is "
        + "three for the product camera, four for speakers, and two for tv.");
  }

  @Test
  public void testGroupByWithWhereClause() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "total");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "store");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "providence");
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "product");
    assertResponse(slots, "The total of the count column in the sales table "
        + "where the value in the store column is equal to providence is three "
        + "for the product speakers, and two for tv.");
  }

  @Test
  public void testGroupByWithWhereClauseOnSameColumn() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "sales");
    addSlotValue(slots, SlotUtil.AGGREGATE, "total");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "count");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "store");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "providence");
    addSlotValue(slots, SlotUtil.GROUP_BY_COLUMN, "store");
    assertResponse(slots, "The total of the count column in the sales table "
        + "where the value in the store column is equal to providence is five "
        + "for the store providence.");
  }
}
