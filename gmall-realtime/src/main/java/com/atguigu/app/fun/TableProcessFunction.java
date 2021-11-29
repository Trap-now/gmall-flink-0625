package com.atguigu.app.fun;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private Connection connection;

    private MapStateDescriptor<String,TableProcess> mapStateDescriptor;     // 通过属性从外面传进来
    private OutputTag<JSONObject> objectOutputTag;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor,OutputTag<JSONObject> objectOutputTag){
        this.mapStateDescriptor=mapStateDescriptor;
        this.objectOutputTag=objectOutputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 加载驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        // 连接地址
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        // 解决先开CDC(CDC使用初始化读取数据)导致原有数据不存在问题
        // 读取Mysql配置表放入内存 map

    }

    // value是由flinkcdc封装的
    // value：{"db":"","tn":"table_process","before":{},"after":{"source_table":""....},"type":""}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 1.解析数据为JavaBean,需要先知道value的格式是什么样子的,解析是对value中的after字段进行解析
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        // 2.校验Hbase表是否存在,如果不存在则建表,需要建表所以需要连接Phonix的连接
        // 首先要判断是Hbase才去建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
                    );
        }

        // 3.将数据写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 定义一个key,用于关联两条流的数据,这里用的是 - ,后面主流中根据主键获取广播状态拼接的时候也要写 -
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);


    }

    // 建表方法,拼接建表语句
    // create table if not exists db.tn(id varchar primary key,tm_name varchar) sinkExtend(有扩展字段再将扩展字段拼上去)
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
        // 防止原本有数据,后期删除了
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        if (sinkPk == null || "".equals(sinkPk)) {
            sinkPk = "id";
        }

        StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        // id什么的需要遍历获取
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            // 取出列名
            String column = columns[i];
            createTableSql.append(column).append(" varchar");

            // 判断是否为主键
            if (column.equals(sinkPk)) {
                createTableSql.append(" primary key");
            }

            // 进行 , 的添加,当当前字段不是最后一个字段时进行添加
            if ( i < columns.length - 1 ) {
                createTableSql.append(",");
            }
        }

        createTableSql.append(")").append(sinkExtend);

        // 打印建表语句
        System.out.println(createTableSql);

        // 执行建表操作,建表语句出错后,就不会往下走了
            preparedStatement = connection.prepareStatement(createTableSql.toString());
            preparedStatement.execute();

            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    // 在这里面将value拿出来,分析一下value的数据格式,跟输出流一样都是从flinkcdc来的,
    // value：{"db":"","tableName":"table_process","before":{},"after":{"id":"","tm_name":"","logo_url":""....},"type":""}

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1.根据主键读取广播状态对应的数据
        String key = value.getString("tableName") + "-" + value.getString("type");
        // 广播变量是个只读变量所以这里是ReadOnly...,因此该返回值没有put方法
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(key);


        // tableProcess 存在为空的可能,因此需要判断一下是否为空再进行操作
        if (tableProcess != null) {
            // 2.根据广播状态数据   过滤字段 logo_url
            filterColumns(value.getJSONObject("after"),tableProcess.getSinkColumns());

            // 3.根据广播状态数据   分流

            value.put("sinkTable",tableProcess.getSinkTable());

            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                ctx.output(objectOutputTag,value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(value);
            }

        } else {
            // 先从内存的map中尝试获取数据

            System.out.println(key + "不存在！");
        }

    }

    private void filterColumns(JSONObject after, String sinkColumns) {
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = after.entrySet();
        // 根据判断结果去remove
        entries.removeIf(next->!columnList.contains(next.getKey()));

    }



}
