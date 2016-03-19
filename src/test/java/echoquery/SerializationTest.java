package echoquery;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.slu.Slot;

import echoquery.querier.QueryRequest;
import echoquery.utils.Serializer;
import echoquery.utils.SlotUtil;

public class SerializationTest {
  private Map<String, Slot> newEmptySlots() {
    Map<String, Slot> slots = new HashMap<>();
    addSlotValue(slots, SlotUtil.TABLE_NAME, null);
    addSlotValue(slots, SlotUtil.FUNC, null);
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

  @Test
  public void test() {
    Map<String, Slot> slots = newEmptySlots();
    addSlotValue(slots, SlotUtil.TABLE_NAME, "employees");
    addSlotValue(slots, SlotUtil.FUNC, "average");
    addSlotValue(slots, SlotUtil.AGGREGATION_COLUMN, "salary");
    addSlotValue(slots, SlotUtil.COMPARISON_COLUMN_1, "name");
    addSlotValue(slots, SlotUtil.COMPARATOR_1, "is not");
    addSlotValue(slots, SlotUtil.COLUMN_VALUE_1, "vinh");
    QueryRequest req = QueryRequest.of(Intent.builder()
          .withName("AggregationIntent")
          .withSlots(slots).build());
    try {
      QueryRequest res =
          (QueryRequest) Serializer.deserialize(Serializer.serialize(req));
      assertEquals(req, res);
    } catch (ClassNotFoundException | IOException e) {
      e.printStackTrace();
    }
  }

}
