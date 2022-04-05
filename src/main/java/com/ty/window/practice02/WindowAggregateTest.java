package com.ty.window.practice02;

import com.ty.datasosurce.ClickSource;
import com.ty.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author ty
 * @date 2022/4/5 11:13
 * @describe 统计对应事件窗口中的用户点击量
 */
public class WindowAggregateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度为1
        sEnv.setParallelism(1);
        // 添加数据源并就近设置水位线
        SingleOutputStreamOperator<Event> input = sEnv
            .addSource(new ClickSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Event>forMonotonousTimestamps()
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                           @Override
                           public long extractTimestamp(Event o, long l) {
                               return o.getTimeStamp();
                           }
                       }
                    ));

        input.print("input");

        SingleOutputStreamOperator<Tuple2<String, Long>> output = input
                .map(event -> Tuple2.of(event.getName(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(tuple2 -> tuple2.f0)
                // 设置滑动事件窗口，窗口时间大小为10s，滑动频率为5s
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new CountAggregate())
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        output.print("CountAggregate");

        sEnv.execute();
    }

    private static class CountAggregate implements AggregateFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> createAccumulator() {
            return Tuple2.of(null, 0L);
        }

        @Override
        public Tuple2<String, Long> add(Tuple2<String, Long> value1, Tuple2<String, Long> value2) {
            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> acc) {
            return Tuple2.of(acc.f0, acc.f1);
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> acc1, Tuple2<String, Long> acc2) {
            return Tuple2.of(acc1.f0, acc1.f1 + acc2.f1);
        }
    }
}
