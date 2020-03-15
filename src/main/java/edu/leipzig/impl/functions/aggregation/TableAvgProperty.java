package edu.leipzig.impl.functions.aggregation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Iterator;

/**
 * Average user-defined aggregate function.
 */
public class TableAvgProperty extends AggregateFunction<PropertyValue, AvgAcc> {

    @Override
    public AvgAcc createAccumulator() {
        return new AvgAcc();
    }

    @Override
    public PropertyValue getValue(AvgAcc acc) {
        if (acc.count == 0L) {
            return PropertyValue.NULL_VALUE;
        } else {
            return PropertyValue.create(acc.sum / acc.count);
        }
    }

    public void accumulate(AvgAcc acc, PropertyValue iValue) {
        if (null != iValue) {
            acc.sum += iValue.getDouble();
            acc.count += 1L;
        }
    }

    public void retract(AvgAcc acc, PropertyValue iValue) {
        if (null != iValue) {
            acc.sum -= iValue.getDouble();
            acc.count -= 1L;
        }
    }

    public void merge(AvgAcc acc, Iterable<AvgAcc> it) {
        Iterator<AvgAcc> iter = it.iterator();
        while (iter.hasNext()) {
            AvgAcc a = iter.next();
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }

    public void resetAccumulator(AvgAcc acc) {
        acc.count = 0L;
        acc.sum = 0;
    }

    @Override
    public TypeInformation getResultType() {
        return TypeInformation.of(PropertyValue.class);
    }

    @Override
    public TypeInformation getAccumulatorType() {
        return TypeInformation.of(AvgAcc.class);
    }
}
