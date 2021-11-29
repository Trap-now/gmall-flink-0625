package com.atguigu.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.management.Descriptor;
import java.text.SimpleDateFormat;

public class test {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取kafka dwd_page_log 主题的数据创建流
        String groupId = "unique_visit_app_210625";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        // TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> map = streamSource.map(JSON::parseObject);

        // TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = map.keyBy(value -> value.getJSONObject("common").getString("mid"));


        // TODO 5.使用状态编程对每天的mid做去重
        SingleOutputStreamOperator<JSONObject> filter = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            // 定义状态对象
            private ValueState<String> valueState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {

                // 初始化状态
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("dt-state", String.class);

                // 设置过期时间大小，周期，...
                StateTtlConfig build = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                // 将过期时间加入到状态中
                stateDescriptor.enableTimeToLive(build);

                valueState = getRuntimeContext().getState(stateDescriptor);

                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 获取
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                // 判断
                if (lastPageId == null) {
                    // 获取当前状态的状态数据
                    String valueSt = valueState.value();
                    // 获取当前状态的时间
                    String ts = simpleDateFormat.format(value.getLong("ts"));
                    if (valueSt == null || !valueSt.equals(ts)) {
                        valueState.update(ts);
                        return true;
                    }

                }

                return false;
            }
        });


        // TODO 6.将数据写入到Kafka
        filter.print();

        filter.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // TODO 7.启动任务
        env.execute();

    }
}
