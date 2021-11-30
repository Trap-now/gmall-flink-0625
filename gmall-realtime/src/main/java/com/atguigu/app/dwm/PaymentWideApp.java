package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;


/**
 * Mock -> Mysql(binLog) -> MaxWell -> Kafka(ods_base_db_m) -> BaseDBApp(修改配置,Phoenix)
 * -> Kafka(dwd_order_info,dwd_order_detail) -> OrderWideApp(关联维度,Redis) -> Kafka(dwm_order_wide)
 * -> PaymentWideApp -> Kafka(dwm_payment_wide)
 */


public class PaymentWideApp {
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

        // TODO 2.从Kafka读取数据 dwd_payment_info  dwm_order_wide
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        DataStreamSource<String> paymentInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));


        // TODO 3.将数据转换为JavaBean并提取时间戳生成WaterMark
        // 使用map进行转换
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoKafkaDS.map(value -> {
            // 将value转换为对应的PaymentInfo对象,不加PaymentInfo.class 会被转换成普通的JSON对象
            PaymentInfo paymentInfo = JSON.parseObject(value, PaymentInfo.class);

            return paymentInfo;
        })
                // 提取时间戳生成WaterMark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        // 这里时间格式会存在问题,需进行转换
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            throw new RuntimeException("时间格式错误！！");
                        }
                    }
                }));

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS.map(value -> JSON.parseObject(value, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                // 这里时间格式会存在问题,需进行转换
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误！！");
                                }
                            }
                        }));


        // TODO 4.将两条流都按照OrderID分组
        KeyedStream<PaymentInfo, Long> paymentInfoLongKeyedStream = paymentInfoDS.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideLongKeyedStream = orderWideDS.keyBy(OrderWide::getOrder_id);

        // TODO 5.双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoLongKeyedStream.intervalJoin(orderWideLongKeyedStream)
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        // 打印测试
        paymentWideDS.print("paymentWideDS>>>>>>>>>>>>");

        // TODO 6.将数据写入Kafka  dwm_payment_wide
        paymentInfoDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        // TODO 7.启动任务
        env.execute();
    }
}
