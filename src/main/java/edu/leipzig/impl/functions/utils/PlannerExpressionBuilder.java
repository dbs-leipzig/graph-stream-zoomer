package edu.leipzig.impl.functions.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.expressions.*;
import scala.collection.Seq;

import java.util.Arrays;


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
     * Current expression object
     */
    protected String currentExpression;

    protected StreamTableEnvironment tableEnv;

    public PlannerExpressionBuilder(StreamTableEnvironment tableEnv) {
        this.tableEnv = tableEnv;
    }

    public String getExpression() {
        return currentExpression;
    }

    /**
     * Sets current expression to a "all fields" expression
     *
     * @return a reference to this object
     */
    public PlannerExpressionBuilder allFields() {
        currentExpression = "*";
        return this;
    }

    /**
     * Sets given expression as current expression
     *
     * @param e expression
     * @return a reference to this object
     */
    public PlannerExpressionBuilder expression(String e) {
        currentExpression = e;
        return this;
    }

    public PlannerExpressionBuilder literal(String e) {
        currentExpression = "'" + e + "'";
        return this;
    }

    /**
     * Sets current expression to a field reference to field with given field name
     *
     * @param fieldName field name
     * @return a reference to this object
     */
    public PlannerExpressionBuilder field(String fieldName) {
        currentExpression = fieldName;
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
        if (!Arrays.asList(tableEnv.listUserDefinedFunctions()).contains(functionName)) {
            tableEnv.registerFunction(functionName, function);
        }
        currentExpression = functionName + "(" + String.join(",", parameters) + ")";
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
        String functionName = function.toString();
        if (!Arrays.asList(tableEnv.listUserDefinedFunctions()).contains(functionName)) {
            tableEnv.registerFunction(functionName, function);
        }
        currentExpression = functionName + "(" + String.join(",", parameters) + ")";
        return this;
    }

    /**
     * Appends an alias to current expression
     *
     * @param name alias name
     * @return a reference to this object
     */
    public PlannerExpressionBuilder as(String name) {
        currentExpression = currentExpression + " as " + name;
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
        currentExpression = "IN(" + String.join(", ", elements);
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
    public PlannerExpressionBuilder and(String expression) {
        if (null == currentExpression) {
            currentExpression = expression;
        } else {
            currentExpression = currentExpression + " AND " + expression;
        }
        return this;
    }

    public PlannerExpressionBuilder row(String... elements) {
        currentExpression = "row(" + String.join(", ", elements) + ")";
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
        currentExpression = currentExpression + " = " + fieldName;
        return this;
    }
}
