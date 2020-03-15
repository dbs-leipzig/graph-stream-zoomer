package edu.leipzig.impl.functions.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.expressions.PlannerExpression;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for a scala sequence of expressions, i.e. {@link Seq < PlannerExpression >} for use with Flink's
 * Table-API. Builder is built upon {@link PlannerExpressionBuilder}.
 * In contrast to the builder for single expressions, each completely built expression is added
 * to a list of expressions.
 * <p>
 * Example usage equivalent to a SQL "foo AS a, bar AS b":
 * <pre>
 *  {@code
 *    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();
 *    builder
 *      .field("foo")
 *      .as("a")
 *      .field("bar")
 *      .as("b")
 *      .buildSeq()
 *  }
 * </pre>
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link ExpressionSeqBuilder
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.util;
 */
public class PlannerExpressionSeqBuilder extends PlannerExpressionBuilder {
    /**
     * Internal list of expressions
     */
    private List<PlannerExpression> expressions;

    /**
     * Constructor
     */
    public PlannerExpressionSeqBuilder() {
        this.expressions = new ArrayList<>();
    }

    /**
     * Returns scala sequence of expressions built with this builder
     *
     * @return scala sequence of expressions
     */
    public Seq<PlannerExpression> buildSeq() {
        appendIfNewExpression();
        return (Seq<PlannerExpression>) expressions;
    }

    /**
     * Returns scala sequence of expressions built with this builder
     *
     * @return scala sequence of expressions
     */
    public PlannerExpression[] buildArray() {
        appendIfNewExpression();
        return ExpressionUtils.convertListToArray(expressions);
    }


    /**
     * Returns java list of expressions built with this builder
     *
     * @return java list of expressions
     */
    public List<PlannerExpression> buildList() {
        appendIfNewExpression();
        return this.expressions;
    }

    /**
     * Returns true if there is no expression built yet
     *
     * @return true if there is no expression built yet
     */
    public boolean isEmpty() {
        appendIfNewExpression();
        return expressions.isEmpty();
    }

    //----------------------------------------------------------------------------
    // Operators which build completely new expressions. Former expression needs
    // to be added to the sequence.
    //----------------------------------------------------------------------------

    @Override
    public PlannerExpressionSeqBuilder allFields() {
        appendIfNewExpression();
        super.allFields();
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder expression(PlannerExpression e) {
        appendIfNewExpression();
        super.expression(e);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder field(String fieldName) {
        appendIfNewExpression();
        super.field(fieldName);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder resolvedField(String fieldName, TypeInformation<?> resultType) {
        appendIfNewExpression();
        super.resolvedField(fieldName, resultType);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder scalarFunctionCall(ScalarFunction function, PlannerExpression[] parameters) {
        appendIfNewExpression();
        super.scalarFunctionCall(function, parameters);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder scalarFunctionCall(ScalarFunction function, String... parameters) {
        appendIfNewExpression();
        super.scalarFunctionCall(function, parameters);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder aggFunctionCall(AggregateFunction function, PlannerExpression[] parameters) {
        appendIfNewExpression();
        super.aggFunctionCall(function, parameters);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder aggFunctionCall(AggregateFunction function, String... parameters) {
        appendIfNewExpression();
        super.aggFunctionCall(function, parameters);
        return this;
    }

    //----------------------------------------------------------------------------
    // Operators which build nested single expressions based on the former
    // expression. No need to add current expression to the sequence.
    //----------------------------------------------------------------------------

    @Override
    public PlannerExpressionSeqBuilder as(String name, Seq<String> extraNames) {
        super.as(name, extraNames);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder as(String name) {
        super.as(name);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder and(PlannerExpression expression) {
        super.and(expression);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder equalTo(PlannerExpression expression) {
        super.equalTo(expression);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder equalTo(String fieldName) {
        super.equalTo(fieldName);
        return this;
    }

    /**
     * Appends the current expression of {@link PlannerExpressionBuilder} to the sequence if it wasn't
     * added already before
     */
    private void appendIfNewExpression() {
        if (null != this.currentExpression && !expressions.contains(this.currentExpression)) {
            expressions.add(this.currentExpression);
        }
    }
}
