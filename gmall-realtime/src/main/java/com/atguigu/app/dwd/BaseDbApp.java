package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.fun.DimSinkFunction;
import com.atguigu.app.fun.MyFlinkCDCDeSer;
import com.atguigu.app.fun.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDbApp {
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

        // TODO 2.获取Kafka ods_base_db 主题创建主流
        // 获取原始数据流
        DataStreamSource<String> dataStream = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db", "base_db_app_210625"));

        // TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(value);
                }
            }
        });

        // TODO 4.过滤删除数据
        // 删除数据在CDC中type类型为delete的
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"delete".equals(value.getString("type"));
            }
        });

        // TODO 5.使用FlinkCDC读取MySQL配置表并创建广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_0625_realtime")
                .tableList("gmall_0625_realtime.table_process")
                // 读取方式,初始化,要将之前的数据也读取出来,这里不能使用earliest,因为是先建的库后开的binlog
                // earliest这种操作要求binlog里面必须有建表语句的操作
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyFlinkCDCDeSer())
                .build();

        // 得到cdc的source
        DataStreamSource<String> cdcDS = env.addSource(sourceFunction);

        // 将cdc的source转换成广播流
        // 看未来往这个广播变量状态中放什么数据来决定泛型
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = cdcDS.broadcast(mapStateDescriptor);


        // TODO 6.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectStream = filterDS.connect(broadcastStream);

        // TODO 7.根据配置流信息处理主流数据(分流)
        // 主流、测输出流都用 JSONObject 因为未来有很多事实数据还需要加工
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase") {
        };

        SingleOutputStreamOperator<JSONObject> kafkaDS = connectStream.process(new TableProcessFunction(mapStateDescriptor, hbaseTag));

        // TODO 8.将HBase数据写出    有很多维度数据需要加工
        // 以JDBC的方式写出去,
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);
        hbaseDS.print("HBase>>>>>");
        hbaseDS.addSink(new DimSinkFunction());

        // TODO 9.将Kafka数据写出    有很多事实数据需要加工
        kafkaDS.print("Kafka>>>>>>");
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(jsonObject.getString("sinkTable"),jsonObject.getString("after").getBytes());
            }
        }));

        // TODO 10.启动任务
        env.execute("BaseDbApp");

    }
}
