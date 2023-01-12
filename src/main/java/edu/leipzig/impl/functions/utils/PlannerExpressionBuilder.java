package edu.leipzig.impl.functions.utils;

import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.expressions.PlannerExpression;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.table.api.Expressions.*;


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
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.util;
 */
public class PlannerExpressionBuilder {
    /**
     * Function name of ExtractPropertyValue scalar function.
     */
    private static final String EXTRACT_PROPERTY_VALUE = "ExtractPropertyValue";

    /**
     * Current expression string
     */
    protected String currentExpressionString;

    /**
     * Current expression object
     */
    protected ApiExpression currentExpression;

    /**
     * A counter for unique attribute names
     */
    private final AtomicInteger attrNameCtr = new AtomicInteger(0);

    /**
     * The table environment
     */
    protected StreamTableEnvironment tableEnv;

    public PlannerExpressionBuilder(StreamTableEnvironment tableEnv) {
        this.tableEnv = tableEnv;
    }

    public Expression getExpression() {
        return currentExpression;
    }

    /**
     * Sets current expression to a "all fields" expression
     *
     * @return a reference to this object
     */
    public PlannerExpressionBuilder allFields() {
        currentExpressionString = "*";
        currentExpression = $("*");
        return this;
    }

    /**
     * Sets given expression as current expression
     *
     * @param e expression
     * @return a reference to this object
     */
    public PlannerExpressionBuilder expression(String e) {
        currentExpressionString = e;
        currentExpression = $(e);
        return this;
    }

    public PlannerExpressionBuilder expression(ApiExpression e) {
        currentExpression = e;
        return this;
    }

    public PlannerExpressionBuilder literal(String e) {
        String literal = "\"" + e + "\"";
        currentExpressionString = literal;
        currentExpression = $(literal);
        return this;
    }

    /**
     * Sets current expression to a field reference to field with given field name
     *
     * @param fieldName field name
     * @return a reference to this object
     */
    public PlannerExpressionBuilder field(String fieldName) {
        currentExpressionString = fieldName;
        currentExpression = $(fieldName);
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
        String functionName = function.toString();
        if(functionName.equals(EXTRACT_PROPERTY_VALUE)) {
            functionName = functionName + attrNameCtr.getAndIncrement();
        }
        if (!Arrays.asList(tableEnv.listUserDefinedFunctions()).contains(functionName)) {
            // Here we use the deprecated api since the PropertyValue type is not a pojo and thus can not be
            // used in the new Flink type system
            registerScalarFunction(tableEnv, function, functionName);
        }
        currentExpressionString = functionName + "(" + String.join(",", parameters) + ")";

        if (parameters.length == 1 && parameters[0] == null) {
            scalarFunctionCall(function);
        } else {
            Expression[] paramExpressions = Arrays.stream(parameters).map(Expressions::$).toArray(Expression[]::new);
            currentExpression = call(functionName, paramExpressions);
        }

        return this;
    }

    public PlannerExpressionBuilder scalarFunctionCall(ScalarFunction function) {
        String functionName = function.toString();
        if(functionName.equals(EXTRACT_PROPERTY_VALUE)) {
            functionName = functionName + attrNameCtr.getAndIncrement();
        }
        if (!Arrays.asList(tableEnv.listUserDefinedFunctions()).contains(functionName)) {
            // Here we use the deprecated api since the PropertyValue type is not a pojo and thus can not be
            // used in the new Flink type system
            registerScalarFunction(tableEnv, function, functionName);
        }
        currentExpression = call(functionName);
        return this;
    }

    public PlannerExpressionBuilder scalarFunctionCall(ScalarFunction function, Expression... parameters) {
        String functionName = function.toString();
        if(functionName.equals(EXTRACT_PROPERTY_VALUE)) {
            functionName = functionName + attrNameCtr.getAndIncrement();
        }
        if (!Arrays.asList(tableEnv.listUserDefinedFunctions()).contains(functionName)) {
            // Here we use the deprecated api since the PropertyValue type is not a pojo and thus can not be
            // used in the new Flink type system
            registerScalarFunction(tableEnv, function, functionName);
        }
        currentExpression = call(functionName, parameters);
        return this;
    }

    private void registerScalarFunction(StreamTableEnvironment tableEnv, ScalarFunction function, String functionName) {
        tableEnv.dropTemporaryFunction(functionName);
        tableEnv.createTemporaryFunction(functionName, function);
    }

    private void registerAggregateFunction(StreamTableEnvironment tableEnv, AggregateFunction function, String functionName) {
        tableEnv.createTemporaryFunction(functionName, function);
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
        String functionName = function.toString();
        if (!Arrays.asList(tableEnv.listUserDefinedFunctions()).contains(functionName)) {
            // Here we use the deprecated api since the PropertyValue type is not a pojo and thus can not be
            // used in the new Flink type system
            registerAggregateFunction(tableEnv, function, functionName);
        }
        currentExpressionString = functionName + "(" + String.join(",", parameters) + ")";
        Expression[] paramExpressions = Arrays.stream(parameters).map(Expressions::$).toArray(Expression[]::new);
        currentExpression = call(functionName, paramExpressions);
        return this;
    }

    public PlannerExpressionBuilder aggFunctionCall(AggregateFunction function, Expression... parameters) {
        String functionName = function.toString();
        if (!Arrays.asList(tableEnv.listUserDefinedFunctions()).contains(functionName)) {
            // Here we use the deprecated api since the PropertyValue type is not a pojo and thus can not be
            // used in the new Flink type system
            registerAggregateFunction(tableEnv, function, functionName);
        }
        currentExpression = call(functionName, parameters);
        return this;
    }

    /**
     * Appends an alias to current expression
     *
     * @param name alias name
     * @return a reference to this object
     */
    public PlannerExpressionBuilder as(String name) {
        currentExpressionString = currentExpressionString + " as " + name;
        currentExpression = currentExpression.as(name);
        return this;
    }

    /**
     * Appends a call of boolean "AND(expression)" operator with given expression to current
     * expression
     * Builds a boolean expression!
     *
     * @param expression expression
     * @return a reference to this object
     */
    public PlannerExpressionBuilder and(ApiExpression expression) {
        if (null == currentExpression) {
            currentExpression = expression;
        } else {
            currentExpression = currentExpression.and(expression);
        }

        return this;
    }

    /**
     * Builds a row expression. Used only as scalar function argument at {@link ToProperties}.
     *
     * @param expressions the expression for the row.
     * @return a reference to this object
     */
    public PlannerExpressionBuilder row(Expression... expressions) {
        Object[] singleElements = new Object[expressions.length];
        for (int i = 0, elementsLength = expressions.length; i < elementsLength; i++) {
            if (i % 2 == 0) {
                singleElements[i] = expressions[i].asSummaryString();
            } else {
                singleElements[i] = expressions[i];
            }
        }
        Object head = singleElements[0];
        Object[] tail = Arrays.copyOfRange(singleElements, 1, singleElements.length);
        currentExpression = Expressions.row(head, tail);
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
        currentExpressionString = currentExpressionString + " = " + fieldName;
        currentExpression = currentExpression.isEqual($(fieldName));
        return this;
    }

    /**
     * Uses the newly introduced AI feature for count. Appends a call to "count()".
     *
     * @return a reference to this object
     */
    public PlannerExpressionBuilder count() {
        currentExpression = lit(1).count();
        return this;
    }

    /**
     * Uses the newly introduced AI feature for sum. Appends a call to "sum()" on the current expression.
     *
     * @return a reference to this object
     */
    public PlannerExpressionBuilder sum(Expression e) {
        currentExpression = ((ApiExpression) e).sum();
        return this;
    }
}
