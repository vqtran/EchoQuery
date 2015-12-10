package echoquery.sql.joins;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

public class InferredContext {

  private Optional<String> aggregation;
  private List<Optional<String>> comparisons;
  private Optional<String> groupBy;

  public InferredContext() {
    aggregation = Optional.empty();
    comparisons = new ArrayList<>();
    groupBy = Optional.empty();
  }

  public InferredContext setAggregationPrefix(
      @Nullable String aggregationTable) {
    aggregation = Optional.ofNullable(aggregationTable);
    return this;
  }

  public InferredContext addComparisonPrefix(@Nullable String comparisonTable) {
    comparisons.add(Optional.ofNullable(comparisonTable));
    return this;
  }

  public InferredContext setGroupBy(String groupByTable) {
    groupBy = Optional.ofNullable(groupByTable);
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
}
