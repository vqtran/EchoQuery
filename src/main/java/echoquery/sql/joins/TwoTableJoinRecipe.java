package echoquery.sql.joins;

import java.util.Optional;

import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Relation;

import echoquery.sql.model.ForeignKey;

public class TwoTableJoinRecipe implements JoinRecipe {
  private String sourceColumn;
  private String sourceTable;
  private String destinationColumn;
  private String destinationTable;


  public TwoTableJoinRecipe(String sourceColumn, String sourceTable,
      String destinationColumn, String destinationTable) {
    this.sourceColumn = sourceColumn;
    this.sourceTable = sourceTable;
    this.destinationColumn = destinationColumn;
    this.destinationTable = destinationTable;
  }

  public TwoTableJoinRecipe(ForeignKey foreignKey) {
    this.sourceColumn = foreignKey.getSourceColumn();
    this.sourceTable = foreignKey.getSourceTable();
    this.destinationColumn = foreignKey.getDestinationColumn();
    this.destinationTable = foreignKey.getDestinationTable();
  }

  @Override
  public boolean isValid() {
    return true;
  }

  @Override
  public Relation render() {
    Relation left = QueryUtil.table(new QualifiedName(this.sourceTable));
    Relation right = QueryUtil.table(new QualifiedName(this.destinationTable));

    Expression leftColumn = new QualifiedNameReference(
        QualifiedName.of(this.sourceTable, this.sourceColumn));
    Expression rightColumn = new QualifiedNameReference(
        QualifiedName.of(this.destinationTable, this.destinationColumn));
    Optional<JoinCriteria> on = Optional.of(
        new JoinOn(new ComparisonExpression(
            ComparisonExpression.Type.EQUAL, leftColumn, rightColumn)));
    return new Join(Join.Type.INNER, left, right, on);
  }

  @Override
  public String wherePrefix() {
    return destinationTable;
  }
}