package echoquery.sql.joins;

import java.util.List;

public class InferredContext {

  private String aggregation;
  
  private List<String> comparisons;
  
  public InferredContext (String aggregation, List<String> comparisons) {
    this.aggregation = aggregation;
    this.comparisons = comparisons;
  }

  public InferredContext addComparisonPrefix(int index, String comparisonColumn) {
    comparisons.add(comparisonColumn);
    return null;
  }

  public InferredContext setAggregationPrefix(String aggregationColumn) {
    aggregation = aggregationColumn;
    return this;
  }
  
  public String getAggregationPrefix() {
    return aggregation;
  }
  
  public String getComparisonPrefix(int index) {
    return comparisons.get(index);
  }

}
