package edu.leipzig.impl.functions.utils;

import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import scala.collection.Seq;

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
    private String expressions = "";

    /**
     * Constructor
     */
    public PlannerExpressionSeqBuilder(StreamTableEnvironment tableEnvironment) {
        super(tableEnvironment);
    }


    public String buildString() {
        appendIfNewExpression();
        return expressions;
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
    public PlannerExpressionSeqBuilder expression(String e) {
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
    public PlannerExpressionSeqBuilder literal(String fieldName) {
        appendIfNewExpression();
        super.literal(fieldName);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder scalarFunctionCall(ScalarFunction function, String... parameters) {
        appendIfNewExpression();
        super.scalarFunctionCall(function, parameters);
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
    public PlannerExpressionSeqBuilder as(String name) {
        super.as(name);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder and(String expression) {
        super.and(expression);
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
        if (null != this.currentExpression && expressions.isEmpty()) {
            expressions = this.currentExpression;
        //} else if (null != this.currentExpression && !expressions.contains(this.currentExpression)) {
        } else if (null != this.currentExpression && !this.currentExpression.isEmpty() && !expressions.endsWith(this.currentExpression)) {
            expressions += "," + this.currentExpression;
        }
    }
}
