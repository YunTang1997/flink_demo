package com.ty.window.practice03;

import com.ty.datasosurce.ClickSource;
import com.ty.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

/**
 * @author ty
 * @date 2022/4/5 15:34
 * @describe 利用全窗口函数计算当前窗口中的UV数
 */
public class WinodwProcessTest {

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

        SingleOutputStreamOperator<String> process = input.keyBy(event -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UVProcessFunction());

        process.print("UVCount");

        sEnv.execute();

    }

    private static class UVProcessFunction extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

        /**
         * 全窗口处理函数内部逻辑
         * @param aBoolean 逻辑分区，key的类型
         * @param context 上下文
         * @param iterable 存储当前窗口所有数据的迭代器
         * @param collector 输出
         * @throws Exception 异常
         */
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
            // 通过hashSet去重
            Set<String> hashSet = new HashSet<>();
            iterable.forEach(event -> hashSet.add(event.getName()));
            // 通过上下文获取window信息
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect("当前窗口时间信息：" + new Timestamp(start) + " ~ " + new Timestamp(end) + ", UV数为： "
                    + hashSet.size());
        }
    }
}
