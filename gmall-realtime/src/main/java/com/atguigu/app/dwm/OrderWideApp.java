package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.fun.DimAsyncFunction;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境设置跟Kafka的分区数保持一致

        //设置状态后端
        //env.setStateBackend(new FsStateBackend(""));
        //开启CK
        //env.enableCheckpointing(5000); //生产环境设置分钟级
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setCheckpointTimeout(10000);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // TODO 2.读取Kafka主题创建流
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group_210625";
        // 添加订单表和订单详情表两个流
        DataStreamSource<String> orderInfoStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));

        // TODO 3.将每行数据转换为JavaBean,并提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(value -> {
            // 这里需要对应Javabean类型,如果不加会被转成普通的JSON对象
            OrderInfo orderInfo = JSON.parseObject(value, OrderInfo.class);

            // 处理OrderInfo中几个由date_time延伸出来的字段
            String create_time = orderInfo.getCreate_time();

            String[] dateTimeArr = create_time.split(" ");
            orderInfo.setCreate_date(dateTimeArr[0]);
            orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
            // 返回最终的orderInfo
            return orderInfo;
        })
                // 提取时间戳
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                })
                );

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(value -> {
            OrderDetail orderDetail = JSON.parseObject(value, OrderDetail.class);

            // 处理OrderDetail中延伸出来的字段
            String create_time = orderDetail.getCreate_time();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
            // 返回最终的orderDetail
            return orderDetail;
        })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                })
                );

        // TODO 4.双流JOIN
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoDS.keyBy(OrderInfo::getId)
                // 进行内连接
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-2), Time.seconds(2))      // 生产环境给定数据最大延迟
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideDS.print("orderWideDS>>>>>>>>>>");

        // TODO 5.关联维度信息
        //orderWideDS.map(value -> {
        // 关联用户维度,通过用户id在Phoenix中查询相关的用户信息,将查询的到的信息补充到要返回的value中
        // 每一个维度表都需要这样做,直接做不好,太麻烦,因此封装一个工具类
        //Long user_id = value.getUser_id();
        // 5.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS =
                AsyncDataStream.unorderedWait(orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        sdf.parse(birthday);

                        long ts = sdf.parse(birthday).getTime();

                        long curTs = System.currentTimeMillis();
                        long age = (curTs - ts) / (1000 * 60 * 60 * 24 * 365L);

                        orderWide.setUser_age((int) age);
                        orderWide.setUser_gender(dimInfo.getString("GENDER"));
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithUserDS.print("User>>>>>>>>");

        // 在运行整个项目时,需要将所有的维度表关联起来,事实表不用,事实表从启动的时候读就可以
        // 维度表需要修改初始化,先启动DB,再启动CDC

        // 5.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getProvince_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                // 字段都应该改为大写,否则关联不上
                System.out.println("DimInfo:" + dimInfo);
                orderWide.setProvince_name(dimInfo.getString("NAME"));
                orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
            }
        }, 60, TimeUnit.SECONDS);
        orderWideWithProvinceDS.print("Province>>>>>>>>>>>>>");

        // 5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSKUDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getSku_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
                orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                orderWide.setTm_id(dimInfo.getLong("TM_ID"));
            }
        }, 60, TimeUnit.SECONDS);

        orderWideWithSKUDS.print("SKU>>>>>>>>>>>>");


        // 5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSPUDS = AsyncDataStream.unorderedWait(
                orderWideWithSKUDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);
        orderWideWithSPUDS.print("SPU>>>>>>>>>>>>>>>>>");

        // 5.5 关联TM维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSPUDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);
        orderWideWithTmDS.print("TM>>>>>>>>>>>>>>");

        // 5.6 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getCategory3_id());
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                orderWide.setCategory3_name(dimInfo.getString("NAME"));
            }
        }, 60, TimeUnit.SECONDS);
        orderWideWithCategory3DS.print("Category3>>>>>>>>>>");


        // TODO 6.将数据写出到Kakfa
        orderWideWithCategory3DS
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));


        // TODO 7.启动任务
        env.execute("OrderWideApp");

    }
}
