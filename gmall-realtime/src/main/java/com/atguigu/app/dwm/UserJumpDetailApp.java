package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置状态后端
        //env.setStateBackend(new FsStateBackend(""));
        //开启CK
        //env.enableCheckpointing(5000); //生产环境设置分钟级
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setCheckpointTimeout(10000);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // TODO 2.读取 kafka 主题数据创建流
        String groupId = "unique_visit_app_210625";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));


        // TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> map = streamSource.map(JSON::parseObject);
        // 处理时间有问题,CEP中用within这个关键字时应该使用事件时间,同时也需要处理乱序数据,因此需要加上乱序时间
        SingleOutputStreamOperator<JSONObject> jsonObjWithDS = map.assignTimestampsAndWatermarks
                (WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        })
                );

        // TODO 4.按照Mid进行分组
        KeyedStream<JSONObject, String> jsonObjDs = jsonObjWithDS.keyBy(value -> value.getJSONObject("common").getString("mid"));

        // TODO 5.定时模式序列

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("last_page_id") == null;
                    }
                }).next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("last_page_id") == null;
                    }
                }).within(Time.seconds(10));

        /*Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String str = value.getJSONObject("common").getString("last_page_id");
                        return str == null || str.length() <= 0;
                    }
                }).times(2)
                .consecutive()
                .within(Time.seconds(10));*/

        // TODO 6.应用模式序列
        PatternStream<JSONObject> patternStream = CEP.pattern(jsonObjDs, pattern);

        // TODO 7.提取时间,匹配上的事件以及超时时间
        OutputTag<JSONObject> timeOut = new OutputTag<JSONObject>("TimeOut"){};
        // 使用什么需要用两个参数的
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOut, new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                }

        );

        // TODO 8.合并事件并写出数据Kafka
        // 满足条件的流与超时流进行合并

        DataStream<JSONObject> timeout = selectDS.getSideOutput(timeOut);
        DataStream<JSONObject> union = selectDS.union(timeout);
        union.print();

        union.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));


        // TODO 9.执行
        env.execute("UserJumpDetailApp");


    }
}
