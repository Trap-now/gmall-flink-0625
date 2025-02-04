package com.atguigu.Test;
;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class FlinkSQLJoinTest {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
                });

        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
                });

        tableEnv.createTemporaryView("t1", bean1DS);
        tableEnv.createTemporaryView("t2", bean2DS);

        //左表：OnCreateAndWrite   右表：OnCreateAndWrite
        //tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 join t2 on t1.id = t2.id").print();

        //左表：OnReadAndWrite     右表：OnCreateAndWrite
        //tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 left join t2 on t1.id = t2.id").print();

        //左表：OnCreateAndWrite   右表：OnReadAndWrite
        //tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 right join t2 on t1.id = t2.id").print();

        //左表：OnReadAndWrite     右表：OnReadAndWrite
        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 full join t2 on t1.id = t2.id").print();

    }

}