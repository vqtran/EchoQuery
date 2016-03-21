package echoquery.querier.infer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Relation;

import echoquery.querier.schema.ForeignKey;

public class MultiTableJoinRecipe implements JoinRecipe {
  private String baseTable;
  private Set<ForeignKey> joins;
  private InferredContext context;

  public MultiTableJoinRecipe(
      String baseTable, Set<ForeignKey> joins, InferredContext context) {
    this.baseTable = baseTable;
    this.joins = joins;
    this.context = context;
  }

  @Override
  public boolean isValid() {
    return true;
  }

  @Override
  public Relation render() {
    List<ForeignKey> keys = new ArrayList<>();
    keys.addAll(joins);
    return render(keys);
  }

  private Relation render(List<ForeignKey> keys) {
    if (keys.isEmpty()) {
      return QueryUtil.table(new QualifiedName(baseTable));
    }
    ForeignKey key = keys.get(0);
    if (keys.size() == 1) {
      return new Join(Join.Type.INNER,
          QueryUtil.table(new QualifiedName(key.getSourceTable())),
          QueryUtil.table(new QualifiedName(key.getDestinationTable())),
          Optional.of(new JoinOn(new ComparisonExpression(
              ComparisonExpression.Type.EQUAL,
              new QualifiedNameReference(QualifiedName.of(
                  key.getSourceTable(), key.getSourceColumn())),
              new QualifiedNameReference(QualifiedName.of(
                  key.getDestinationTable(), key.getDestinationColumn()))))));
    }
    return new Join(Join.Type.INNER,
        render(keys.subList(1, keys.size())),
        QueryUtil.table(new QualifiedName(key.getDestinationTable())),
        Optional.of(new JoinOn(new ComparisonExpression(
            ComparisonExpression.Type.EQUAL,
            new QualifiedNameReference(QualifiedName.of(
                key.getSourceTable(), key.getSourceColumn())),
            new QualifiedNameReference(QualifiedName.of(
                key.getDestinationTable(), key.getDestinationColumn()))))));
  }

  @Override
  public InferredContext getContext() {
    return context;
  }

  @Override
  public InvalidJoinRecipe.Error getReason() {
    return null;
  }

  @Override
  public List<String> getPossibleTables() {
    return null;
  }

  @Override
  public String getInvalidColumn() {
    return null;
  }

}