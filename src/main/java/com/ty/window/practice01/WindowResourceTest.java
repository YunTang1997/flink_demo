package com.ty.window.practice01;

import com.ty.datasosurce.ClickSource;
import com.ty.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author tangyun
 * @date 2022/4/4 5:39 下午
 */
public class WindowResourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间特性，当前版本默认就是"时间时间"
        // sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Event> input = sEnv.addSource(new ClickSource());
        // 与数据源就近设置水位线
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = input
                // 由于数据源为非乱序数据，则设置为非乱序水位线策略：forMonotonousTimestamps，或则forBoundedOutOfOrderness(Duration.ZERO)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                // 从数据源Event对象中选择timeStamp字段来设置水位线
                                return event.getTimeStamp();
                            }
                        }));

        SingleOutputStreamOperator<Tuple2<String, Long>> returns = eventSingleOutputStreamOperator
                // 通过map将数据转化为Tuple2结构
                .map(event -> Tuple2.of(event.getName(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                // 分区
                .keyBy(tuple2 -> tuple2.f0)
                // 设置时间滚动窗口，窗口时间长度10s
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 通过reduce函数统计每10s内窗口用户的点击量
                .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        returns.print("count-click");

        sEnv.execute();
    }
}
