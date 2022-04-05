package com.ty.window.practice03;

import com.ty.datasosurce.ClickSource;
import com.ty.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * @author tangyun
 * @date 2022/4/5 4:30 下午
 */
public class UVCountExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        SingleOutputStreamOperator<Event> input = sEnv.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTimeStamp();
                            }
                        }));

        input.print("input");

        SingleOutputStreamOperator<String> output = input.keyBy(event -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 通过aggregate和ProcessWindowFunction两者相结合计算UV
                .aggregate(new UVAgg(), new UVCount());

        output.print("uvCount");

        sEnv.execute();
    }

    private static class UVAgg implements AggregateFunction<Event, HashSet<String>, Integer> {
        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> acc) {
            acc.add(event.getName());
            return acc;
        }

        @Override
        public Integer getResult(HashSet<String> acc) {
            return acc.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }

    private static class UVCount extends ProcessWindowFunction<Integer, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Integer> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            // 此处iterator的数据来自UVAgg的getResult函数
            Integer uv = iterable.iterator().next();
            collector.collect("当前窗口时间信息：" + new Timestamp(start) + " ~ " + new Timestamp(end) + ", UV数为： "
                    + uv);
        }
    }
}
