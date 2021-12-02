package com.atguigu.Test;

import jdk.nashorn.internal.ir.EmptyNode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.time.Duration;

public class TestJoin1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("localhost", 8888)
                .map(value -> {
                    String[] split = value.split(",");
                    return new Bean1(split[0], split[1], Long.parseLong(split[2]));
                });
        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("localhost", 9999)
                .map(value -> {
                    String[] split = value.split(",");
                    return new Bean2(split[0], split[1], Long.parseLong(split[2]));
                });
        SingleOutputStreamOperator<Bean1> bean1WithWM = bean1DS.assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
            @Override
            public long extractTimestamp(Bean1 element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));
        SingleOutputStreamOperator<Bean2> bean2WithWM = bean2DS.assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
            @Override
            public long extractTimestamp(Bean2 element, long recordTimestamp) {
                return element.getTs() * 1000;
            }
        }));

        bean1WithWM.keyBy(Bean1::getId)
                .intervalJoin(bean2WithWM.keyBy(Bean2::getId))
                .between(Time.seconds(-5),Time.seconds(5))
                .process(new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1,Bean2>>() {
                    @Override
                    public void processElement(Bean1 bean1, Bean2 bean2, Context ctx, Collector<Tuple2<Bean1, Bean2>> out) throws Exception {
                        out.collect(new Tuple2<>(bean1,bean2));
                    }
                }).print();

        env.execute();


    }
}
