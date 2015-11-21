package echoquery.sql.joins;

import java.util.ArrayList;
import java.util.List;

public class InferredContext {

  private String aggregation;
  private List<String> comparisons;

  public InferredContext() {
    comparisons = new ArrayList<>();
  }

  public InferredContext(String aggregation, List<String> comparisons) {
    this.aggregation = aggregation;
    this.comparisons = comparisons;
  }

  public InferredContext setAggregationPrefix(String aggregationColumn) {
    aggregation = aggregationColumn;
    return this;
  }

  public InferredContext addComparisonPrefix(String comparisonColumn) {
    comparisons.add(comparisonColumn);
    return this;
  }

  public String getAggregationPrefix() {
    return aggregation;
  }

  public String getComparisonPrefix(int index) {
    return comparisons.get(index);
  }
}
