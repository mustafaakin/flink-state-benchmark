/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.mustafaakin;

import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.rocksdb.RocksDB;


public class StreamingJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateBackend state = new RocksDBStateBackend("file:///tmp/file-state-backend");
        env.setStateBackend(state);

        DataStream<Tuple3<Integer, Double, Byte[]>> source = env.addSource(new FakeDataSource(15, 10, 1024 * 1024));

        ProcessWindowFunction<Tuple3<Integer, Double, Byte[]>, String, Integer, TimeWindow> fn = new ProcessWindowFunction<Tuple3<Integer, Double, Byte[]>, String, Integer, TimeWindow>() {
            @Override
            public void process(Integer integer, Context context, Iterable<Tuple3<Integer, Double, Byte[]>> elements, Collector<String> out) throws Exception {
                AverageAccumulator a = new AverageAccumulator();
                int count = 0;
                long data = 0;
                for (Tuple3<Integer, Double, Byte[]> t : elements) {
                    count++;
                    a.add(t.f1);
                    data = data + t.f2.length;
                }

                String msg = String.format("Key %d, Count: %d Len: %.2f KMB Avg: %.2f", integer, count, (data / 1024.0), a.getLocalValue());
                out.collect(msg);
            }
        };


        source.keyBy(t -> t.f0)
                .timeWindow(Time.seconds(10))
                .process(fn)
                .print();

        env.execute("Flink Streaming Java API Skeleton");
    }
}
