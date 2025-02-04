package com.atguigu.app.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.fun.MyFlinkCDCDeSer;
import com.atguigu.utils.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class Flink_CDCWithCustomerSchema {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink_0625")
//                .tableList("gmall_flink_0625.z_user_info")
                //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
                // 第一次用initial()即可,后续使用latest()
                //.startupOptions(StartupOptions.initial())
                .startupOptions(StartupOptions.latest())    // 生产环境不用改,生产环境有CK,每次重启都会从CK开始进行
                .deserializer(new MyFlinkCDCDeSer())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);


        //将数据发送至Kafka
        streamSource.print();
        streamSource.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        env.execute();
    }
}
