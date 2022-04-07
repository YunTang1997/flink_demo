package com.ty.combinestream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ty
 * @date 2022/4/7 22:29
 * @describe 实时对账的需求
 */
public class BillCheckExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        // 模拟用户通过app下单数据流
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream =
                sEnv.fromElements(Tuple3.of("order-1", "app", 1000L), Tuple3.of("order-2", "app", 2000L))
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (tuple3, l) -> tuple3.f2));

        // 模拟第三方收到用户付款记录流
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartStream =
                sEnv.fromElements(Tuple4.of("order-1", "third-party", "success", 3000L), Tuple4.of("order-3", "third-party", "success", 4000L))
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple4<String, String, String, Long>>) (tuple4, l) -> tuple4.f3));

        // 使用connect连接两条流，xxStream1.connect(xxStream2)，则意味着在coProcessFunction中xxStream1数据调用xx1()，xxStream2调用xx2()
        appStream
                // 对两条数据流通过同一个key进行分区，并以相同key进行连接
                .connect(thirdPartStream)
                .keyBy(tuple3 -> tuple3.f0, tuple4 -> tuple4.f0)
                .process(new OrderMatchResult()).print();

        sEnv.execute();
    }

    private static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            appEventState = getRuntimeContext().getState(new ValueStateDescriptor<>("app-event",
                    Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            thirdPartState = getRuntimeContext().getState(new ValueStateDescriptor<>("thirdParty-event",
                    Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context context, Collector<String> collector) throws Exception {
            // 对账成功
            if (thirdPartState.value() != null) {
                collector.collect("对账成功：" + value + " " + thirdPartState.value());
                // 清空第三方状态
                thirdPartState.clear();
            } else {
                // 更新app状态
                appEventState.update(value);
                // 设置定时器，等待5s，看第三方数据是否
                context.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context context, Collector<String> collector) throws Exception {
            // 对账成功
            if (appEventState.value() != null) {
                collector.collect("对账成功：" + appEventState.value() + " " + value);
                // 清空app状态
                appEventState.clear();
            } else {
                // 更新第三方状态
                thirdPartState.update(value);
                // 设置定时器，等待app状态
                context.timerService().registerEventTimeTimer(value.f3);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (appEventState.value() != null) {
                out.collect("对账失败：" + appEventState.value() + " " + "第三方支付平台信息未到");
            }

            if (thirdPartState.value() != null) {
                out.collect("对账失败：" + thirdPartState.value() + " " + "app信息未到");
            }

            appEventState.clear();
            thirdPartState.clear();
        }
    }
}
