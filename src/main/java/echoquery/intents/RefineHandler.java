package echoquery.intents;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.speech.slu.Intent;
import com.amazon.speech.speechlet.Session;
import com.amazon.speech.speechlet.SpeechletResponse;
import com.google.common.base.Optional;

import echoquery.querier.Querier;
import echoquery.querier.QueryRequest;
import echoquery.querier.infer.SchemaInferrer;
import echoquery.querier.schema.ColumnName;
import echoquery.querier.schema.ColumnType;
import echoquery.utils.RefineType;
import echoquery.utils.Response;
import echoquery.utils.Serializer;
import echoquery.utils.SessionUtil;
import echoquery.utils.SlotUtil;

/**
 * Where refine logic takes place. Modifies the QueryRequest from the previous
 * interaction that was stored in the Session object to add/replace/drop
 * where clauses and group-bys as needed by the user. Funnels the new request
 * back to QueryHandler.
 */
public class RefineHandler implements IntentHandler {

  private final Querier querier;
  private final QueryHandler queryHandler;

  public RefineHandler(Connection conn, QueryHandler queryHandler) {
    this.querier = new Querier(conn);
    this.queryHandler = queryHandler;
  }

  @Override
  public SpeechletResponse respond(Intent intent, Session session) {
    try {
      return queryHandler.respond(
          refine(intent, (QueryRequest) Serializer.deserialize(
              (String) session.getAttribute(SessionUtil.REQUEST_ATTRIBUTE))),
          session);
    } catch (ClassNotFoundException | IOException e) {
      e.printStackTrace();
      return Response.unexpectedError(session);
    }
  }

  /**
   * Exposed for testing purposes - SpeechletResponse is impossible to inspect.
   */
  public String getResponseInEnglish(Intent intent, Session session) {
    QueryRequest previousRequest;
    try {
      previousRequest = (QueryRequest) Serializer.deserialize(
                (String) session.getAttribute(SessionUtil.REQUEST_ATTRIBUTE));
      return querier.execute(refine(intent, previousRequest)).getMessage();
    } catch (ClassNotFoundException | IOException e) {
      e.printStackTrace();
      return "There was an error generating the response in English";
    }
  }

  /**
   * Refines the where clauses and group-bys according to what the user
   * specified in the intent slots.
   *
   * @param intent
   * @param previousRequest
   * @return
   */
  private QueryRequest refine(Intent intent, QueryRequest previousRequest) {
    // First get what type of refinement this will be.
    RefineType refineType = SlotUtil.getRefineType(
        intent.getSlot(SlotUtil.REFINE_TYPE).getValue());

    /**
     * 1. Refine: GROUP-BYs. This is easy so let's get this out of the way first
     */
    if (intent.getSlot(SlotUtil.GROUP_BY_COLUMN).getValue() != null) {
      // We only support one group-by so add and replace will both replace.
      if (refineType == RefineType.ADD || refineType == RefineType.REPLACE) {
        return previousRequest
            .setGroupByColumn(SlotUtil.parseColumnSlot(
                intent.getSlot(SlotUtil.GROUP_BY_COLUMN).getValue()));
      }
      // Drop will overwrite the group-by with an empty column.
      if (refineType == RefineType.DROP) {
        return previousRequest.setGroupByColumn(new ColumnName());
      }
    }

    /**
     * 2. Refine: WHEREs. This is much less trivial.
     *
     * First collect all the columns, comparators, and values into proper lists.
     */
    List<ColumnName> comparisonColumns = new ArrayList<>();
    List<String> comparators = new ArrayList<>();
    List<String> comparisonValues = new ArrayList<>();

    if (intent.getSlot(SlotUtil.COMPARISON_COLUMN_1).getValue() != null) {
      comparisonColumns.add(SlotUtil.parseColumnSlot(
          intent.getSlot(SlotUtil.COMPARISON_COLUMN_1).getValue())
              .setType(
                  (intent.getSlot(SlotUtil.COLUMN_NUMBER_1).getValue() != null)
                  ? ColumnType.NUMBER : ColumnType.STRING));
      comparators.add(intent.getSlot(SlotUtil.COMPARATOR_1).getValue());
      comparisonValues.add(ObjectUtils.defaultIfNull(
          intent.getSlot(SlotUtil.COLUMN_VALUE_1).getValue(),
          intent.getSlot(SlotUtil.COLUMN_NUMBER_1).getValue()));
    }
    if (intent.getSlot(SlotUtil.COMPARISON_COLUMN_2).getValue() != null) {
      comparisonColumns.add(SlotUtil.parseColumnSlot(
          intent.getSlot(SlotUtil.COMPARISON_COLUMN_2).getValue())
              .setType(
                  (intent.getSlot(SlotUtil.COLUMN_NUMBER_2).getValue() != null)
                  ? ColumnType.NUMBER : ColumnType.STRING));
      comparators.add(intent.getSlot(SlotUtil.COMPARATOR_2).getValue());
      comparisonValues.add(ObjectUtils.defaultIfNull(
          intent.getSlot(SlotUtil.COLUMN_VALUE_2).getValue(),
          intent.getSlot(SlotUtil.COLUMN_NUMBER_2).getValue()));
    }
    if (intent.getSlot(SlotUtil.COMPARISON_COLUMN_3).getValue() != null) {
      comparisonColumns.add(SlotUtil.parseColumnSlot(
          intent.getSlot(SlotUtil.COMPARISON_COLUMN_3).getValue())
              .setType(
                  (intent.getSlot(SlotUtil.COLUMN_NUMBER_3).getValue() != null)
                  ? ColumnType.NUMBER : ColumnType.STRING));
      comparators.add(intent.getSlot(SlotUtil.COMPARATOR_3).getValue());
      comparisonValues.add(ObjectUtils.defaultIfNull(
          intent.getSlot(SlotUtil.COLUMN_VALUE_3).getValue(),
          intent.getSlot(SlotUtil.COLUMN_NUMBER_3).getValue()));
    }

    /**
     * Delegate each type of refine to helpers since they all have different
     * logic.
     */
    switch (refineType) {
      case ADD:
        return refineAddWheres(
            previousRequest, comparisonColumns, comparators, comparisonValues);
      case REPLACE:
        return refineReplaceWheres(
            previousRequest, comparisonColumns, comparators, comparisonValues);
      case DROP:
        return refineDropWheres(
            previousRequest, comparisonColumns, comparators, comparisonValues);
    }

    return previousRequest;
  }

  /**
   * The Refine ADD case is simple, it simply adds all the where clauses to the
   * end. We assume that they are joined via ANDs.
   *
   * @param previousRequest
   * @param comparisonColumns
   * @param comparators
   * @param comparisonValues
   * @return
   */
  private QueryRequest refineAddWheres(
      QueryRequest previousRequest,
      List<ColumnName> comparisonColumns,
      List<String> comparators,
      List<String> comparisonValues) {
    for (int i = 0; i < comparisonColumns.size(); i++) {
      previousRequest.addWhereClause(
          previousRequest.getComparisonColumns().size() > 0 ? "and" : null,
          comparisonColumns.get(i),
          comparators.get(i),
          comparisonValues.get(i));
    }
    return previousRequest;
  }

  /**
   * The Refine REPLACE case is the most complicated. Basically: if all the
   * referenced columns exist already in the query, we want to simply replace
   * the comparator and value for the clauses referenced by those columns
   * (keeping clauses that were unreferenced by the refine.) BUT, if the refine
   * has a column that isn't in any clause already in the query, then drop all
   * of the existing where clauses in favor of these new ones given in the
   * refinement.
   *
   * @param previousRequest
   * @param comparisonColumns
   * @param comparators
   * @param comparisonValues
   * @return
   */
  private QueryRequest refineReplaceWheres(
      QueryRequest previousRequest,
      List<ColumnName> comparisonColumns,
      List<String> comparators,
      List<String> comparisonValues) {

    /**
     * 1. Determine if the refinement refers to any columns that doesn't already
     *    exist in the query's where clauses.
     */
    boolean hasNewCols = false;
    for (ColumnName col : comparisonColumns) {
      Optional<ColumnName> match =
          findMatch(col, previousRequest.getComparisonColumns());
      if (!match.isPresent()) {
        hasNewCols = true;
        break;
      }
    }

    /**
     * 2. If it does contain new columns in where clauses, then we want to drop
     *    all where clauses in favor of these new where clauses.
     */
    if (hasNewCols) {
      previousRequest.removeAllWhereClauses();
      refineAddWheres(
          previousRequest, comparisonColumns, comparators, comparisonValues);
    }
    /**
     * 3. If it doesn't, we want to replace the comparator and value for each
     *    of the where clauses referenced by the columns in the refinement.
     */
    else {
      for (int i = 0; i < comparisonColumns.size(); i++) {
        Optional<ColumnName> match = findMatch(
            comparisonColumns.get(i), previousRequest.getComparisonColumns());
        previousRequest.modifyWhereClause(
            match.get(), comparators.get(i), comparisonValues.get(i));
      }
    }

    return previousRequest;
  }

  /**
   * The Refine DROP case is simple. Removes all where clauses in the query
   * with column references matching those in the refinement.
   *
   * @param previousRequest
   * @param comparisonColumns
   * @param comparators
   * @param comparisonValues
   * @return
   */
  private QueryRequest refineDropWheres(
      QueryRequest previousRequest,
      List<ColumnName> comparisonColumns,
      List<String> comparators,
      List<String> comparisonValues) {
    for (int i = 0; i < comparisonColumns.size(); i++) {
      Optional<ColumnName> match = findMatch(
          comparisonColumns.get(i), previousRequest.getComparisonColumns());
      if (match.isPresent()) {
        previousRequest.removeWhereClause(match.get());
      }
    }
    return previousRequest;
  }

  /**
   * A helper function to find a ColumnName match from a list of ColumnNames.
   * A "match" is defined specially here to be either an exact match, or if
   * toMatch's table is unknown, if there is a match of column names only.
   *
   * This latter assumption is reasonable (but clearly flawed) because someone
   * who clarified "age" to be "patient age" in the past shouldn't have to
   * clarify "age" again.
   *
   * TODO(vqtran): Use the schema inferrer here to have more intelligent
   *    matching and clarification for these ambiguous ColumnNames. This opens
   *    up a whole can of software engineering worms though.
   *
   * @param toMatch
   * @param cols
   * @return
   */
  private Optional<ColumnName> findMatch(
      ColumnName toMatch, List<ColumnName> cols) {
    for (ColumnName col : cols) {
      if (toMatch.equals(col)
          || (!toMatch.getTable().isPresent()
              && toMatch.getColumn().equals(col.getColumn()))) {
        return Optional.of(col);
      }
    }
    return Optional.absent();
  }
}
