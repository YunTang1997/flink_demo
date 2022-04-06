package com.ty.process.practice01;

import com.ty.datasosurce.ClickSource;
import com.ty.entity.Event;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author ty
 * @date 2022/4/6 19:58
 * @describe 实现处理时间定时器
 */
public class ProcessingTimeTimerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        SingleOutputStreamOperator<String> output = sEnv.addSource(new ClickSource())
                .keyBy(event -> true)
                // 定时器只能在keyedStream中使用
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
                        // 获取timeService
                        TimerService timerService = context.timerService();
                        // 获取当前处理时间
                        long currentTime = timerService.currentProcessingTime();
                        // 注册定时器，时间为：当前处理时间+10s
                        timerService.registerProcessingTimeTimer(currentTime + 10 * 1000L);
                        collector.collect("数据到达，当前时间为：" + new Timestamp(currentTime));
                    }

                    /**
                     * 定时器所设定的处理时间到达后，要触发的逻辑
                     * @param timestamp 设定好的触发时间（事件时间语义下就是由水位线（watermark）来触发）
                     * @param ctx 继承自ProcessedFunction的上下文Context，也即processElement函数中的Context
                     * @param out 输出
                     * @throws Exception 异常
                     */
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器逻辑触发，当前处理时间为：" + new Timestamp(timestamp));
                    }
                });

        output.print("ProcessingTimeTimer");

        sEnv.execute();
    }
}
