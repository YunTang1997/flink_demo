package com.ty.process.practice01;

import com.ty.datasosurce.ClickSource;
import com.ty.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author ty
 * @date 2022/4/6 20:22
 * @describe 实现事件时间定时器
 */
public class EventTimeTimerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        SingleOutputStreamOperator<Event> input = sEnv.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTimeStamp();
                            }
                        }));

        SingleOutputStreamOperator<String> output = input.keyBy(event -> event.getName()).process(new KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
                // 获取逻辑分区key值
                String key = context.getCurrentKey();
                TimerService timerService = context.timerService();
                // 获取水位线
                long watermark = timerService.currentWatermark();
                // 获取当前时间，注意和ProcessingTimeTimerTest的区别
                long currentTime = context.timestamp();
                // 注册事件时间定时器
                timerService.registerEventTimeTimer(currentTime + 10 * 1000L);
                // watermark的更新会滞后于当前数据的timestamp（watermark更新=前一次数据的timestamp-1毫秒）
                collector.collect("数据到达，当前时间为：" + new Timestamp(currentTime) + ", watermark为："
                        + new Timestamp(watermark));
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                // 获取水位线（注意当数据流终止之后，即不再有新数据传入，此时watermark会被置为Long.MAX_VALUE，则所有定时器均会触发）
                long watermark = ctx.timerService().currentWatermark();
                out.collect("定时器逻辑触发，当前处理时间为：" + new Timestamp(timestamp) + ", watermark为："
                        + new Timestamp(watermark));
            }
        });

        output.print("EventTime");

        sEnv.execute();
    }
}
