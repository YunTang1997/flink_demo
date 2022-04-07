package com.ty.window.practice03;

import com.ty.datasosurce.ClickSource;
import com.ty.entity.Event;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * @author ty
 * @date 2022/4/6 21:37
 * @describe
 */
public class TopNExample_ProcessAllWindowFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        SingleOutputStreamOperator<Event> input = sEnv.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimeStamp();
                    }
                }));

        input
            .map(event -> event.getUrl())
            // 开启全局滑动窗口，并设置窗口事件长度为10s，滑动频率为5s
            .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult())
            .print("Top-Three");

        sEnv.execute();

    }

    private static class UrlHashMapCountAgg implements AggregateFunction<String, Map<String, Long>, List<Tuple2<String, Long>>> {
        @Override
        public Map<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, Long> add(String s, Map<String, Long> acc) {
            if (acc.containsKey(s)) {
                acc.put(s, acc.get(s) + 1L);
            } else {
                acc.put(s, 1L);
            }

            return acc;
        }

        @Override
        public List<Tuple2<String, Long>> getResult(Map<String, Long> map) {
            List<Tuple2<String, Long>> res = new ArrayList<>();
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                res.add(Tuple2.of(entry.getKey(), entry.getValue()));
            }

            // 按照url点击量倒叙排序
            res.sort((o1, o2) -> (int) (o2.f1 - o1.f1));
            return res;
        }

        @Override
        public Map<String, Long> merge(Map<String, Long> acc0, Map<String, Long> acc1) {
            return null;
        }
    }

    private static class UrlAllWindowResult extends ProcessAllWindowFunction<List<Tuple2<String, Long>>, List<Tuple2<String, Long>>, TimeWindow> {

        @Override
        public void process(Context context, Iterable<List<Tuple2<String, Long>>> iterable, Collector<List<Tuple2<String, Long>>> collector) throws Exception {
            List<Tuple2<String, Long>> list = iterable.iterator().next();
            // 输出url点击量topN
            // 注意报错：Caused by: java.lang.ClassNotFoundException: scala.Option，
            // 则需要在global libraries中配置scala-sdk版本同pom文件中flink依赖中scala版本一致
            collector.collect(list);
        }
    }
}
