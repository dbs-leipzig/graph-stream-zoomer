package edu.leipzig.impl.functions.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for a scala sequence of expressions, i.e. {@link Seq < Expression >} for use with Flink's
 * Table-API. Builder is built upon {@link ExpressionBuilder}.
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
public class ExpressionSeqBuilder extends ExpressionBuilder {
    /**
     * Internal list of expressions
     */
    private List<Expression> expressions;

    /**
     * Constructor
     */
    public ExpressionSeqBuilder() {
        this.expressions = new ArrayList<>();
    }

    /**
     * Returns scala sequence of expressions built with this builder
     *
     * @return scala sequence of expressions
     */
    public Seq<Expression> buildSeq() {
        appendIfNewExpression();
        return ExpressionUtils.convertToSeq(expressions);
    }

    /**
     * Returns java list of expressions built with this builder
     *
     * @return java list of expressions
     */
    public List<Expression> buildList() {
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
    public ExpressionSeqBuilder allFields() {
        appendIfNewExpression();
        super.allFields();
        return this;
    }

    @Override
    public ExpressionSeqBuilder expression(Expression e) {
        appendIfNewExpression();
        super.expression(e);
        return this;
    }

    @Override
    public ExpressionSeqBuilder field(String fieldName) {
        appendIfNewExpression();
        super.field(fieldName);
        return this;
    }

    @Override
    public ExpressionSeqBuilder resolvedField(String fieldName, TypeInformation<?> resultType, int fieldIndex) {
        appendIfNewExpression();
        super.resolvedField(fieldName, resultType, fieldIndex);
        return this;
    }

    @Override
    public ExpressionSeqBuilder scalarFunctionCall(ScalarFunction function, Expression[] parameters) {
        appendIfNewExpression();
        super.scalarFunctionCall(function, parameters);
        return this;
    }

    @Override
    public ExpressionSeqBuilder scalarFunctionCall(ScalarFunction function, String... parameters) {
        appendIfNewExpression();
        super.scalarFunctionCall(function, parameters);
        return this;
    }

    @Override
    public ExpressionSeqBuilder aggFunctionCall(AggregateFunction function, Expression[] parameters) {
        appendIfNewExpression();
        super.aggFunctionCall(function, parameters);
        return this;
    }

    @Override
    public ExpressionSeqBuilder aggFunctionCall(AggregateFunction function, String... parameters) {
        appendIfNewExpression();
        super.aggFunctionCall(function, parameters);
        return this;
    }

    //----------------------------------------------------------------------------
    // Operators which build nested single expressions based on the former
    // expression. No need to add current expression to the sequence.
    //----------------------------------------------------------------------------

    @Override
    public ExpressionSeqBuilder as(String name, Seq<String> extraNames) {
        super.as(name, extraNames);
        return this;
    }

    @Override
    public ExpressionSeqBuilder as(String name) {
        super.as(name);
        return this;
    }

    @Override
    public ExpressionSeqBuilder and(Expression expression) {
        super.and(expression);
        return this;
    }

    @Override
    public ExpressionSeqBuilder equalTo(Expression expression) {
        super.equalTo(expression);
        return this;
    }

    @Override
    public ExpressionSeqBuilder equalTo(String fieldName) {
        super.equalTo(fieldName);
        return this;
    }

    /**
     * Appends the current expression of {@link ExpressionBuilder} to the sequence if it wasn't
     * added already before
     */
    private void appendIfNewExpression() {
        if (null != this.currentExpression && !expressions.contains(this.currentExpression)) {
            expressions.add(this.currentExpression);
        }
    }
}
