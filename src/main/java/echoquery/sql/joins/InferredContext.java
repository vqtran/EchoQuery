package echoquery.sql.joins;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferredContext {

  private String aggregation;
  
  private List<String> comparisons;

  private static final Logger log = LoggerFactory.getLogger(InferredContext.class);
  
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
