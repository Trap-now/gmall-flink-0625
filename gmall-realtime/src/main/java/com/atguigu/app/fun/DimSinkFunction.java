package com.atguigu.app.fun;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    // 连接的声明
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 加载驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        // 创建连接
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // value的格式,根据value的格式
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 进行连接的初始化,在这里不能关闭连接,当前并行度只有这一个连接,如果在这里关了,第一条数据来了之后就会关闭,后续数据没有连接了
        PreparedStatement preparedStatement = null;
        String sinkTable = value.getString("sinkTable");
        JSONObject after = value.getJSONObject("after");
        try {
            // 拼接SQL语句：upsert into db.tn(id,name,sex) values(1001,zhangsan,male)
            String upsertSql = genUpsertSql(sinkTable, after);
            System.out.println(upsertSql);


            // 编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            // 如果数据更新了需要将Redis中的数据删掉
            // 这里使用的是Phoenix的表名 sinkTable ,这里需要转化为大写,因为Phoenix中也是大写
            if ("update".equals(value.getString("type"))) {
                DimUtil.deleteDimInfo(sinkTable.toUpperCase(),after.getString("id"));
            }


            // 执行
            preparedStatement.execute();

            // 提交
            connection.commit();

        } catch (SQLException e) {
            System.out.println("写入" + sinkTable + "表数据失败！");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    private String genUpsertSql(String sinkTable, JSONObject after) {
        // 获取列名      [id,name,sex]  =>  "id,name,sex"      mkString(list,",")
        Set<String> columns = after.keySet();

        // 获取列值
        Collection<Object> values = after.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')";
    }

}
