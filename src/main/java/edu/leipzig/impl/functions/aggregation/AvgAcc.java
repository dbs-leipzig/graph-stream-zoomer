package edu.leipzig.impl.functions.aggregation;

/**
 * Accumulator for Avg.
 */
public class AvgAcc {
    double sum = 0D;
    long count = 0L;

    public AvgAcc() {
    }

    public AvgAcc(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
