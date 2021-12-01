package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.ClickHoustUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
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


        // TODO 2.消费 3 个主题的数据创建流
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> pageViewDStream = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uniqueVisitDStream = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpDStream = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));


        // TODO 3.将数据格式化成统一的 Javabean 对象
        SingleOutputStreamOperator<VisitorStats> pvDS = pageViewDStream.map(value -> {
            JSONObject jsonObject = JSON.parseObject(value);
            JSONObject common = jsonObject.getJSONObject("common");

            JSONObject page = jsonObject.getJSONObject("page");
            String lastPageId = page.getString("last_page_id");

            long sv = 0L;
            if (lastPageId == null) {
                sv = 1L;
            }

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    jsonObject.getLong("ts")
            );

        });

        SingleOutputStreamOperator<VisitorStats> uvDS = uniqueVisitDStream.map(value -> {
            JSONObject jsonObject = JSON.parseObject(value);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats(
                    "", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts")
            );
        });

        SingleOutputStreamOperator<VisitorStats> ujDS = userJumpDStream.map(value -> {
            JSONObject jsonObject = JSON.parseObject(value);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats(
                    "", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 1L,
                    jsonObject.getLong("ts")
            );

        });

        // TODO 4.将 3 个流进行 UNION
        DataStream<VisitorStats> unionDS = pvDS.union(uvDS, ujDS);


        // TODO 5.提取事件时间生成 watermark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(13))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // TODO 6.分组、开窗、聚合
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowStream = visitorStatsWithWmDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getAr(), value.getCh(), value.getVc(), value.getIs_new());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> result = windowStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());

                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                // 取出数据
                VisitorStats visitorStats = input.iterator().next();

                // 补充窗口时间
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                // 输出数据
                out.collect(visitorStats);

            }
        });




        // TODO 7.将数据写出到 clickhouse
        // flink 本身提供了一个 jdbc 连接器,但是 Phoenix 用不了,因为一个流中会包含很多张表的数据,而这些表的字段、字段个数都不一样
        // jdbc 连接器更适合单表写

        result.print("result>>>>>>>>>");
        // 使用 jdbcsink需要导入相关的jdbc依赖
        // sink()中的参数：
        // sql:sql语句;
        // statementBuilder:给占位符赋值;
        // executionOptions:执行条件,关于批处理的内容;
        // connectionOptions:连接参数,提供的Driver、URL;
        // 后两个参数都是统一的,可以通过写一个工具类后期进行调用
        // result.addSink(JdbcSink.sink())

        // 使用工具类调用获取方法
        // 建表字段顺序与Javabean的顺序是一样的
        // 因此这里不用写字段名,只需要保证values()括号中问号的位置与表中字段的位置是一一对应的个数和类型
        ClickHoustUtil.getJdbcSink("insert into visitor_stats_210625 values(?,?,?,?,?,?,?,?,?,?,?,?)");

        // TODO 8.启动任务
        env.execute();
    }
}
