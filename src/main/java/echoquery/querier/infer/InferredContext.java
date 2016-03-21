package echoquery.querier.infer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Optional;

public class InferredContext implements Serializable {
  /**
   * Generated for serialization.
   */
  private static final long serialVersionUID = 8912725560514264222L;

  private Optional<String> aggregation;
  private List<Optional<String>> comparisons;
  private Optional<String> groupBy;

  public InferredContext() {
    aggregation = Optional.absent();
    comparisons = new ArrayList<>();
    groupBy = Optional.absent();
  }

  public InferredContext setAggregationPrefix(
      @Nullable String aggregationTable) {
    aggregation = Optional.fromNullable(aggregationTable);
    return this;
  }

  public InferredContext addComparisonPrefix(@Nullable String comparisonTable) {
    comparisons.add(Optional.fromNullable(comparisonTable));
    return this;
  }

  public InferredContext setGroupBy(String groupByTable) {
    groupBy = Optional.fromNullable(groupByTable);
    return this;
  }

  public Optional<String> getAggregationPrefix() {
    return aggregation;
  }

  public List<Optional<String>> getComparisons() {
    return comparisons;
  }

  public Optional<String> getComparisonPrefix(int index) {
    return comparisons.get(index);
  }

  public Optional<String> getGroupByPrefix() {
    return groupBy;
  }

  public Set<String> distinctTables() {
    Set<String> tables = new HashSet<>();
    if (aggregation.isPresent()) {
      tables.add(aggregation.get());
    }
    for (Optional<String> comparison : comparisons) {
      if (comparison.isPresent()) {
        tables.add(comparison.get());
      }
    }
    if (groupBy.isPresent()) {
      tables.add(groupBy.get());
    }
    return tables;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result =
        prime * result + ((aggregation == null) ? 0 : aggregation.hashCode());
    result =
        prime * result + ((comparisons == null) ? 0 : comparisons.hashCode());
    result =
        prime * result + ((groupBy == null) ? 0 : groupBy.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    InferredContext other = (InferredContext) obj;
    if (aggregation == null) {
      if (other.getAggregationPrefix() != null)
        return false;
    } else if (!aggregation.equals(other.getAggregationPrefix()))
      return false;
    if (comparisons == null) {
      if (other.getComparisons() != null)
        return false;
    } else if (!comparisons.equals(other.getComparisons()))
      return false;
    if (groupBy == null) {
      if (other.getGroupByPrefix() != null)
        return false;
    } else if (!groupBy.equals(other.getGroupByPrefix()))
      return false;
    return true;
  }
}
