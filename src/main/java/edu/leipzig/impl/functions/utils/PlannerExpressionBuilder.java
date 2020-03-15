package edu.leipzig.impl.functions.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.expressions.*;
import scala.collection.Seq;


/**
 * Builder for a single (eventually nested) Flink Table {@link PlannerExpression} for use with Flink's
 * Table-API
 * <p>
 * Example usage equivalent to a SQL "foo AS bar":
 * <pre>
 *  {@code
 *    ExpressionBuilder builder = new ExpressionBuilder();
 *    builder
 *      .field("foo")
 *      .as("bar")
 *      .toExpression()
 *  }
 * </pre>
 * <p>
 * The builder does not perform any semantic check! Using correct operators is delegated to
 * the user.
 * Calling specific methods on a builder instance may lead to undo proceeding calls. Example:
 *
 * <pre>
 *  {@code
 *    ExpressionBuilder builder = new ExpressionBuilder();
 *    builder
 *      .field("foo")
 *      .as("bar")
 *      .field("foo2")
 *      .toExpression()
 *  }
 * </pre>
 * <p>
 * The returned expression will only contain the field reference "foo2".
 * <p>
 * Note it is possible to build senseless expressions like "foo AS bar IN (foo) AS bar".
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link ExpressionBuilder
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.util;
 */
public class PlannerExpressionBuilder {
    /**
     * Current expression object
     */
    protected PlannerExpression currentExpression;

    // protected ResolvedFieldReference resolvedFieldReference;

    /**
     * Returns built expression object
     *
     * @return flink expression object
     */
    public PlannerExpression toExpression() {
        return currentExpression;
    }

    /**
     * Sets current expression to a "all fields" expression
     *
     * @return a reference to this object
     */
    public PlannerExpressionBuilder allFields() {
        currentExpression = new UnresolvedFieldReference("*");
        return this;
    }

    /**
     * Sets given expression as current expression
     *
     * @param e expression
     * @return a reference to this object
     */
    public PlannerExpressionBuilder expression(PlannerExpression e) {
        currentExpression = e;
        return this;
    }

    /**
     * Sets current expression to a field reference to field with given field name
     *
     * @param fieldName field name
     * @return a reference to this object
     */
    public PlannerExpressionBuilder field(String fieldName) {
        currentExpression = new UnresolvedFieldReference(fieldName);
        return this;
    }

    /**
     * Sets current expression to a field reference to field with given field name with given type
     *
     * @param fieldName  field name
     * @param resultType field type
     * @return a reference to this object
     */
    public PlannerExpressionBuilder resolvedField(String fieldName, TypeInformation<?> resultType) {
        this.currentExpression =  new PlannerResolvedFieldReference(fieldName, resultType);
        return this;
    }

    /**
     * Sets current expression to a call of given scalar function with given parameters
     *
     * @param function   table scalar function
     * @param parameters array of expressions as {@link PlannerExpression}
     * @return a reference to this object
     */
    public PlannerExpressionBuilder scalarFunctionCall(ScalarFunction function, PlannerExpression[] parameters) {
        currentExpression = new PlannerScalarFunctionCall(function, ExpressionUtils.convertToSeq(parameters));
        return this;
    }

    /**
     * Sets current expression to a call of given scalar function with given field names as parameters
     *
     * @param function   table scalar function
     * @param parameters array of field names
     * @return a reference to this object
     */
    public PlannerExpressionBuilder scalarFunctionCall(ScalarFunction function, String... parameters) {
        return scalarFunctionCall(function,
                ExpressionUtils.convertStringArrayToFieldReferenceArray(parameters));
    }

    /**
     * Sets current expression to a call of given aggregation function with given parameters
     *
     * @param function   table aggregation function
     * @param parameters array of expressions as {@link PlannerExpression}
     * @return a reference to this object
     */
    public PlannerExpressionBuilder aggFunctionCall(AggregateFunction function, PlannerExpression[] parameters) {
        currentExpression = new AggFunctionCall(function, function.getResultType(),
                function.getAccumulatorType(), ExpressionUtils.convertToSeq(parameters));
        return this;
    }

    /**
     * Sets current expression to a call of given aggregation function with given field names as
     * parameters
     *
     * @param function   table aggregation function
     * @param parameters array of field names
     * @return a reference to this object
     */
    public PlannerExpressionBuilder aggFunctionCall(AggregateFunction function, String... parameters) {
        return aggFunctionCall(function,
                ExpressionUtils.convertStringArrayToFieldReferenceArray(parameters));
    }

    /**
     * Appends an alias to current expression
     *
     * @param name       alias name
     * @param extraNames extra names
     * @return a reference to this object
     */
    public PlannerExpressionBuilder as(String name, Seq<String> extraNames) {
        currentExpression = new Alias(currentExpression, name, extraNames);
        return this;
    }

    /**
     * Appends an alias to current expression
     *
     * @param name alias name
     * @return a reference to this object
     */
    public PlannerExpressionBuilder as(String name) {
        return as(name, ExpressionUtils.EMPTY_STRING_SEQ);
    }

    /**
     * Appends a call of SQL "IN(expressions...)" with given expressions to current expression
     * Builds a boolean expression!
     *
     * @param elements array of expressions
     * @return a reference to this object
     */
    public PlannerExpressionBuilder in(PlannerExpression... elements) {
        currentExpression = new In( currentExpression, ExpressionUtils.convertToSeq(elements));
        return this;
    }

    /**
     * Appends a call of SQL "IN('foo', 'bar', ..)" with given string literals to current expression
     * Builds a boolean expression!
     *
     * @param elements array of string literals
     * @return a reference to this object
     */
    public PlannerExpressionBuilder in(String... elements) {
        return in(ExpressionUtils.convertStringArrayToLiteralArray(elements));
    }


    /**
     * Appends a call of boolean "AND(expression)" operator with given expression to current
     * expression
     * Builds a boolean expression!
     *
     * @param expression expression
     * @return a reference to this object
     */
    public PlannerExpressionBuilder and(PlannerExpression expression) {
        if (null == currentExpression) {
            currentExpression = expression;
        } else {
            currentExpression = new And(currentExpression, expression);
        }
        return this;
    }

    /**
     * Appends a call of boolean "=" operator with given expression to current expression
     * Builds a boolean expression!
     *
     * @param expression expression
     * @return a reference to this object
     */
    public PlannerExpressionBuilder equalTo(PlannerExpression expression) {
        currentExpression = new EqualTo(currentExpression, expression);
        return this;
    }

    /**
     * Appends a call of boolean "=" operator with given field name to current expression
     * Builds a boolean expression!
     *
     * @param fieldName field name
     * @return a reference to this object
     */
    public PlannerExpressionBuilder equalTo(String fieldName) {
        return equalTo(new UnresolvedFieldReference(fieldName));
    }
}
