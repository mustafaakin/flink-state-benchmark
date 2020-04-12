package dev.mustafaakin;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class FakeDataSource implements SourceFunction<Tuple3<Integer,Double,Byte[]>> {
    private final int range;
    private final int sleep;
    private final int size;

    public FakeDataSource(int range, int sleep, int size) {
        this.range = range;
        this.sleep = sleep;
        this.size =size;
    }

    @Override
    public void run(SourceContext<Tuple3<Integer,Double,Byte[]>> sourceContext) throws Exception {
        Random random = new Random(System.currentTimeMillis());

        while (true) {
            Byte[] data = new Byte[this.size];
            Tuple3<Integer, Double, Byte[]> t = Tuple3.of(random.nextInt(this.range), random.nextDouble(), data);
            sourceContext.collect(t);
            Thread.sleep(random.nextInt(this.sleep));
        }
    }

    @Override
    public void cancel() {

    }
}
