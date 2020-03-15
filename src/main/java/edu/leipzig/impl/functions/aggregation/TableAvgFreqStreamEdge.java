package edu.leipzig.impl.functions.aggregation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.sql.Timestamp;

public class TableAvgFreqStreamEdge extends AggregateFunction<PropertyValue, AvgFreqAcc> {
    @Override
    public AvgFreqAcc createAccumulator() {
        AvgFreqAcc acc = new AvgFreqAcc();
        acc.start = new Timestamp(System.currentTimeMillis()).getTime();
        acc.count = 0L;
        return acc;
    }

    @Override
    public PropertyValue getValue(AvgFreqAcc accumulator) {
        if (accumulator.count == 0L) {
            return PropertyValue.NULL_VALUE;
        } else {
            long now = new Timestamp(System.currentTimeMillis()).getTime();
            double diffInSec = (now - accumulator.start) / 1000D;
            if(diffInSec == 0L){
                return PropertyValue.create(0L);
            }else{
                return PropertyValue.create(accumulator.count / diffInSec);
            }
        }
    }

    public void accumulate(AvgFreqAcc acc, PropertyValue iValue) {
            acc.count += 1L;
    }

    public void retract(AvgFreqAcc acc, PropertyValue iValue) {
            acc.count -= 1L;
    }

    @Override
    public TypeInformation getResultType() {
        return TypeInformation.of(PropertyValue.class);
    }

    @Override
    public TypeInformation getAccumulatorType() {
        return TypeInformation.of(AvgFreqAcc.class);
    }
}
