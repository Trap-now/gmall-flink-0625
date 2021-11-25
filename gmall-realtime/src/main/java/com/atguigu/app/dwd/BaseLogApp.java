package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// 测试需要做的两件事情
// 数据流:目前是行为数据, 架构：web/App前端埋点 -> Nginx -> 日志服务器 -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
// 程  序:                        Mock       -> Nginx -> Logger.sh -> kf.sh(zk.sh) ->BaseLogApp -> kf

public class BaseLogApp {
    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);      //生产环境设置跟Kafka的分区数保持一致,一个slot专门消费一个分区的内容,多了少了都不好

        /*// 设置状态后端(生产环境中一定要做的事情),,需要用到HDFS
        // 状态后端默认的是memory,至少要用fs/rockdb(rockdb需要单独导入依赖)
        // 如果这里不写默认读的是flink-conf中的参数
        env.setStateBackend(new FsStateBackend(""));
        // 开启checkpoint
        // 这里的checkpoint是两个CK之间的间隔,头跟头
        env.enableCheckpointing(5000);  // 生产环境设置分钟级,这里做测试所以时间比较短
        // 设置相关参数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);   // 最多几个CK同时存在
        env.getCheckpointConfig().setCheckpointTimeout(10000);      // 超时时间
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 设置检查点模式*/

        // TODO 2.消费Kafka ods_base_log 主题数据
        // 这里需要用到消费者组相关的参数,防止每次都写一遍
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_log_app_210625"));

        // TODO 3.过滤脏数据 使用侧输出流
        // 外部的outputTag,没有转成功的写入到测输出流,所以格式是String
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                // 将数据转换成JSONObject进行输出
                try {
                    JSONObject jsonObject = JSON.parseObject(value);    // 如果value是非json字符串会出现异常
                    out.collect(jsonObject);                            // 没有异常,正常写出
                } catch (Exception e) {
                    // 抓到异常写入到测输出流,后期还要在外部提取当前测输出流的数据
                    ctx.output(outputTag, value);
                }
            }
        });

        // 提取测输出流中的脏数据
        DataStream<String> sideOutput = jsonObjectDS.getSideOutput(outputTag);
        sideOutput.print("Dirty>>>>>>");

        // TODO 4.按照 Mid 分组
        // 根据数据格式可知,需要先获取common字段,然后获取mid,得到一个根据 mid 进行分组的流
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        // TODO 5.新老用户校验  状态编程
        // 老用户的 is_new 字段是 1 还是 0
        // 泛型依旧是JSONObject,只是将用户的 1 转换成了 0
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            // 定义一个状态对其进行初始化
            private ValueState<String> valueState;

            // 在生命周期方法中进行初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                // 获取 common 中的 is_new 字段
                String isNew = value.getJSONObject("common").getString("is_new");
                // 判断是否为 1
                if ("1".equals(isNew)) {
                    // 取出状态数据
                    String state = valueState.value();

                    if (state != null) {
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        valueState.update("0");
                    }
                }
                return value;
            }
        });

        // TODO 6.分流  使用测输出流    三条流：启动、页面、曝光
        // 如果使用状态编程,方法中需要跟第三方数据库进行交互,至少需要rich,因为需要获取上下文运行状态和声明周期方法
        // 还有创建连接的问题,生命周期方法创建连接,一个slot里面共用这一个连接
        // 普通的就是最普通的,rich多了生命周期方法和运行时上下文,process多了测输出流和定时器,但是windowProcessFunction中没有定时器
        // 需要往kafka中转,转入的类型以及确认是String,因此process的输出泛型也是String

        OutputTag<String> startOutPutTag = new OutputTag<String>("start"){};
        OutputTag<String> displayOutPutTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                // 获取启动数据,不需要对数据进行加工,因此直接获取就可以了
                String start = value.getString("start");
                // 这里第二个判断条件是根据公司数据格式来的,有的会只有两个括号没有数据,此时不是null
                // 大的格式分为启动和页面,页面中包括是否曝光
                if (start != null && start.length() > 2) {
                    // 启动日志,将数据写入启动日志侧输出流
                    ctx.output(startOutPutTag, value.toJSONString());
                } else {
                    // 非启动日志,页面日志,将数据写入主流
                    out.collect(value.toJSONString());

                    // 获取曝光数据,曝光数据在数据中的格式是数组的类型,因此需要获取数组类型格式的数据
                    JSONArray displays = value.getJSONArray("displays");
                    // 获取曝光数据后,对数据进行遍历,因此需要判断数据是否为空
                    if (displays != null && displays.size() > 0) {
                        // 提取页面ID
                        String pageId = value.getJSONObject("page").getString("page_id");

                        // 遍历写出
                        for (int i = 0; i < displays.size(); i++) {
                            // 如果每次只对每一条数据进行写出,数据太少,不利于后期数据调用
                            JSONObject display = displays.getJSONObject(i); // 单独一条一条的曝光数据
                            // 给数据添加一些信息,需要什么就添加什么
                            display.put("page_id", pageId);

                            // 将数据写入曝光的侧输出流
                            ctx.output(displayOutPutTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        // TODO 7.将 3 个流写入对应的Kafka主题
        DataStream<String> startDS = pageDS.getSideOutput(startOutPutTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutPutTag);

        pageDS.print("Page>>>>>>");
        startDS.print("Start>>>>>>>");
        displayDS.print("Display>>>>>>>>");

        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        // TODO 8.启动任务
        env.execute("BaseLogApp");

    }
}
