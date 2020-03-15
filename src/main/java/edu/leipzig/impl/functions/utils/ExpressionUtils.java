package edu.leipzig.impl.functions.utils;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.planner.expressions.Literal;
import org.apache.flink.table.planner.expressions.PlannerExpression;
import org.apache.flink.table.planner.expressions.UnresolvedFieldReference;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Utils to work with Flink's table expressions and scala sequences of expressions.
 * <p>
 * This implementation reuses much of the code of Much of Grable.
 * the code is copied directly or has only small changes.
 *
 * @link ExpressionUtils
 * <p>
 * references to: org.gradoop.flink.model.impl.layouts.table.util;
 */
public class ExpressionUtils {
    /**
     * Empty scala sequence of strings
     */
    public static final Seq<String> EMPTY_STRING_SEQ = JavaConverters
            .asScalaIteratorConverter((new ArrayList<String>()).iterator()).asScala().toSeq();

    /**
     * Converts given array of expressions to a scala sequence of expressions
     *
     * @param array array of expressions
     * @return scala sequence of expressions
     */
    public static Seq<PlannerExpression> convertToSeq(PlannerExpression[] array) {
        return convertToSeq(new ArrayList<PlannerExpression>(Arrays.asList(array)));
    }

    /**
     * Converts given collection of expressions to a scala sequence of expressions
     *
     * @param collection collection of expressions
     * @return scala sequence of expressions
     */
    public static Seq<PlannerExpression> convertToSeq(Collection<PlannerExpression> collection) {
        return JavaConverters.asScalaIteratorConverter(collection.iterator()).asScala().toSeq();
    }

    /**
     * Converts given array of field name strings to a array of field reference expressions
     *
     * @param array array of field name strings
     * @return array of field reference expressions
     */
    public static PlannerExpression[] convertStringArrayToFieldReferenceArray(String... array) {
        return Arrays.stream(array).map(fieldName -> new UnresolvedFieldReference(fieldName))
                .toArray(PlannerExpression[]::new);
    }


    /**
     * Converts given array of string literals to a array of literal expressions
     *
     * @param array array of string literals
     * @return array of literal expressions
     */
    public static PlannerExpression[] convertStringArrayToLiteralArray(String... array) {
        return Arrays.stream(array)
                .map(str -> new Literal(str, Types.STRING)).toArray(PlannerExpression[]::new);
    }

    public static PlannerExpression[] convertListToArray(List<PlannerExpression> expressions) {
        return expressions.toArray(PlannerExpression[]::new);
    }
}
