/*
 * Copyright © 2021 - 2023 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.dbsleipzig.stream.grouping.impl.functions.utils;

import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
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
     * todo: check if this can be removed
     */
    private String expressionsString = "";

    List<Expression> expressions = new ArrayList<>();

    /**
     * Constructor
     */
    public PlannerExpressionSeqBuilder(StreamTableEnvironment tableEnvironment) {
        super(tableEnvironment);
    }

    public Expression[] build() {
        appendIfNewExpression();
        return expressions.toArray(new Expression[0]);
    }

    /**
     * Returns true if there is no expression built yet
     *
     * @return true if there is no expression built yet
     */
    public boolean isEmpty() {
        appendIfNewExpression();
        return expressionsString.isEmpty() || expressions.isEmpty();
    }

    //----------------------------------------------------------------------------
    // Operators which build completely new expressions. Former expression needs
    // to be added to the sequence.
    //----------------------------------------------------------------------------

    @Override
    public PlannerExpressionSeqBuilder expression(String e) {
        appendIfNewExpression();
        super.expression(e);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder expression(ApiExpression e) {
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
    public PlannerExpressionSeqBuilder scalarFunctionCall(ScalarFunction function) {
        appendIfNewExpression();
        super.scalarFunctionCall(function);
        return this;
    }

    @Override
    public PlannerExpressionSeqBuilder scalarFunctionCall(ScalarFunction function, Expression... parameters) {
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

    @Override
    public PlannerExpressionSeqBuilder aggFunctionCall(AggregateFunction function, Expression... parameters) {
        appendIfNewExpression();
        super.aggFunctionCall(function, parameters);
        return this;
    }

    @Override
    public PlannerExpressionBuilder count() {
        appendIfNewExpression();
        super.count();
        return this;
    }

    @Override
    public PlannerExpressionBuilder sum(Expression e) {
        appendIfNewExpression();
        super.sum(e);
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
            expressions.add(this.currentExpression);
        } else if (null != this.currentExpression && !expressions.contains(this.currentExpression)) {
            expressions.add(this.currentExpression);
        }

        if (null != this.currentExpressionString && expressionsString.isEmpty()) {
            expressionsString = this.currentExpressionString;
        //} else if (null != this.currentExpression && !expressions.contains(this.currentExpression)) {
        } else if (null != this.currentExpressionString && !this.currentExpressionString
          .isEmpty() && !expressionsString.endsWith(this.currentExpressionString)) {
            expressionsString += "," + this.currentExpressionString;
        }
    }
}
