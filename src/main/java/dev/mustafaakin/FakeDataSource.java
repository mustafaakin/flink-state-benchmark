package dev.mustafaakin;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FakeDataSource implements SourceFunction<Tuple3<Integer, Double, Byte[]>> {
    private final int range;
    private final int sleep;
    private final int size;
    private Byte[] data;

    public FakeDataSource(int range, int sleep, int size) {
        this.range = range;
        this.sleep = sleep;
        this.size = size;
        this.data = new Byte[size];
    }

    private final static AtomicLong c = new AtomicLong();


    @Override
    public void run(SourceContext<Tuple3<Integer, Double, Byte[]>> sourceContext) throws Exception {
        Random random = new Random(System.currentTimeMillis());

        while (true) {
            int x = random.nextInt(this.range);
            Tuple3<Integer, Double, Byte[]> t = Tuple3.of(x, random.nextDouble(), data);
            sourceContext.collect(t);
            c.incrementAndGet();
        }
    }

    public static long get() {
        return c.get();
    }

    @Override
    public void cancel() {

    }
}
