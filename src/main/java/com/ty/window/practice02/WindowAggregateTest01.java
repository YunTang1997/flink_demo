package com.ty.window.practice02;

import com.ty.datasosurce.ClickSource;
import com.ty.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;
import java.util.Set;

/**
 * @author ty
 * @date 2022/4/5 14:35
 * @describe 计算实时数据中用户的平均点击数（pv / uv）
 */
public class WindowAggregateTest01 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        SingleOutputStreamOperator<Event> input = sEnv.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.getTimeStamp();
                                    }
                                }));

        input.print("input");

        SingleOutputStreamOperator<Double> output = input
                // 全部数据分配到一个窗口，但是数据量过大时，会产生数据倾斜问题
                .keyBy(event -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new PvAndUv());

        output.print("PvAndUv");

        sEnv.execute();
    }

    private static class PvAndUv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {
        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> tuple2) {
            tuple2.f1.add(event.getName());
            return Tuple2.of(tuple2.f0 + 1L, tuple2.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> tuple2) {
            return (double) tuple2.f0 / tuple2.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> acc0, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }
}
