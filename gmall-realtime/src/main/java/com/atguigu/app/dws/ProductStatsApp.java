package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.fun.DimAsyncFunction;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentWide;
import com.atguigu.bean.ProductStats;
import com.atguigu.common.GmallConfig;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHoustUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.runtime.fs.hdfs.HadoopBlockLocation;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hbase.thirdparty.io.netty.util.collection.CharObjectMap;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
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

        // TODO 2.读取Kafka 7个主题数据创建流
        String groupId = "product_stats_app_210625";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> pageViewDStream = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorInfoDStream = env.addSource(MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideDStream = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentWideDStream = env.addSource(MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId));
        DataStreamSource<String> cartInfoDStream = env.addSource(MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId));
        DataStreamSource<String> refunInfoDStream = env.addSource(MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentInfoDStream = env.addSource(MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId));

        // TODO 3.将 7 个流转换为统一的数据格式
        // 3.1处理点击与曝光数据
        /*
         这里处理数据不能使用 map ,点击和曝光放在一起,进来一个数据可能是点击也可能是曝光
         一条 sku_id 一条数据,如果一条数据中包含点击和曝光,输出也是一条,无法区分是哪种类型,因此至少使用flatmap
         压平操作,一进多出
          */
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS  = pageViewDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                // 将数据转换为 JSON 对象
                JSONObject jsonObject = JSON.parseObject(value);

                // flatmap处理出来的数据不一定是点击数据,在平台上点击商品后会跳到商品详情页
                // 取出时间戳
                Long ts = jsonObject.getLong("ts");

                // 取出页面数据
                JSONObject page = jsonObject.getJSONObject("page");
                String item_type = jsonObject.getString("item_type");
                String page_id = page.getString("page_id");
                // 判断当前行为是否是点击商品行为
                if ("sku_id".equals(item_type) && "good_detail".equals(page_id)) {
                    // 使用构造者设计模式获取需要的属性
                    out.collect(
                            ProductStats.builder()
                                    .sku_id(page.getLong("item"))   // 商品
                                    .click_ct(1L)       // 点击次数
                                    .ts(ts)             // 时间
                                    .build()
                    );
                }

                // 取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");

                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {

                        // 取出单挑曝光数据
                        JSONObject display = displays.getJSONObject(i);

                        // 页面中曝光出来的不一定是个商品也有可能是个广告
                        // 判断当前曝光是广告还是商品还是其它的
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(
                                    ProductStats.builder()
                                            .sku_id(display.getLong("item"))
                                            .click_ct(1L)
                                            .ts(ts)
                                            .build()
                            );
                        }
                    }
                }
            }
        });

        // 3.2 处理收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavoDS = favorInfoDStream.map(value -> {
            JSONObject jsonObject = JSON.parseObject(value);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.3 处理加购数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(value -> {
            JSONObject jsonObject = JSON.parseObject(value);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.4 处理订单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDStream.map(value -> {
            // 订单数据定义的有Javabean 所以直接转换成Javabean格式的返回值然后直接调用即可
            OrderWide orderWide = JSON.parseObject(value, OrderWide.class);

            HashSet<Long> orderIds = new HashSet<>();

            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getSplit_total_amount())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();

        });

        // 3.5 处理支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = paymentWideDStream.map(value -> {
            PaymentWide paymentWide = JSON.parseObject(value, PaymentWide.class);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();

        });

        // 3.6 处理退单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithRefunDS = refunInfoDStream.map(value -> {
            JSONObject jsonObject = JSON.parseObject(value);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.7 处理评价数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(value -> {
            JSONObject jsonObject = JSON.parseObject(value);

            long goodCt = 0;

            if (GmallConstant.APPRAISE_GOOD.equals(jsonObject.getString("appraise"))) {
                goodCt = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // TODO 4.将 7 个流进行UNION
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavoDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefunDS,
                productStatsWithCommentDS
        );

        // TODO 5.提取时间戳生成watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                            @Override
                            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // TODO 6.分组、开窗、聚合
        SingleOutputStreamOperator<ProductStats> productStatsDS = productStatsWithWMDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {
                        value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                        value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());
                        value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());
                        value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());
                        value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        //value1.setOrder_ct(value1.getOrderIdSet().size() + 0L);
                        value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());
                        value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));

                        value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());
                        //value1.setRefund_order_ct(value1.getRefundOrderIdSet().size() + 0L);
                        value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));

                        value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                        //value1.setPaid_order_ct(value1.getPaidOrderIdSet().size() + 0L);

                        value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                        value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());
                        return value1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                        // 取出数据
                        ProductStats productStats = input.iterator().next();

                        // 补充窗口时间
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                        // 补充订单个数
                        productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                        productStats.setRefund_order_ct((long) productStats.getOrderIdSet().size());
                        productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());

                        // 输出数据
                        out.collect(productStats);
                    }
                });

        // TODO 7.关联维度
        // 7.1 SKU
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                productStatsDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfo.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                    }
                }
                , 60, TimeUnit.SECONDS
        );

        // 7.2 SPU
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream = AsyncDataStream.unorderedWait(
                productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getSpu_id());
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        // 7.3 Category
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream = AsyncDataStream.unorderedWait(
                productStatsWithSpuDstream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getCategory3_id());
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setCategory3_name(dimInfo.getString("NAME"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        // 7.4 TM
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream = AsyncDataStream.unorderedWait(
                productStatsWithCategory3Dstream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS
        );

        // TODO 8.将数据写出到clickhouse
        productStatsWithTmDstream.print(">>>>>>>>>>");
        productStatsWithTmDstream.addSink(ClickHoustUtil.getJdbcSink("insert into product_stats_210625 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 9.启动任务
        env.execute("ProductStatsApp");
    }
}
