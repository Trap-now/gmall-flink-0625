package com.atguigu.app.dwm;

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

import java.text.SimpleDateFormat;

public class UniqueVisitApp {
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

        // TODO 2.读取 Kafka dwd_page_log 主题的数据创建流
        String groupId = "unique_visit_app_210625";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kfDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        // TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> map = kfDS.map(JSON::parseObject);

        // TODO 4.按照Mid进行分组
        KeyedStream<JSONObject, String> keyedStream = map.keyBy(value -> value.getJSONObject("common").getString("mid"));

        // TODO 5.使用状态编程对每天的数据进行去重
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            // 定义状态对象,泛型为String
            private ValueState<String> valueState;
            private SimpleDateFormat sdf;


            // 先执行
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("dt-state", String.class);

                // 设置过期时间大小及过期时间方式
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        // 设置当前状态根据哪种行为进行过期时间的更新,在创建和写的时候进行更新
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                // 将过期时间加入到这个状态中
                stateDescriptor.enableTimeToLive(stateTtlConfig);

                // 将运行时上下文状态赋给状态对象
                valueState = getRuntimeContext().getState(stateDescriptor);

                // 定义时间格式
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 获取上一条页面,根据上一条页面中last_page_id这条数据的状态来判断该用户是否是当天第一次登录的用户
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                /*
                 判断逻辑：
                         当lastPageId为空的时候说明该条数据是第一次出现,不为空则不是第一次出现过滤掉
                            取出lastPageId第一次出现的状态数据,根据状态进行判断：判断是当天第一次登录还是
                                判断状态数据是否为空,为空则保留,不为空则过滤掉
                                判断当前状态数据的时间是否是当前时间,是当前时间则过滤,否则保留
                                    同时需更新状态对象的时间为当前时间
                  */


                if (lastPageId == null) {   // 为空一定是今天没有登录过的
                    // 取出当天这条数据的状态数据
                    String visitDt = valueState.value();

                    // 取出当前数据的日期
                    String ts = sdf.format(value.getLong("ts"));

                    if (visitDt == null || !ts.equals(visitDt)) {
                        // 更新这条数据的时间
                        valueState.update(ts);
                        return true;
                    }
                }
                return false;
            }
        });


        // TODO 6.将数据写出kafka
        uvDS.print();
        uvDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // TODO 7.启动任务
        env.execute();
    }
}
