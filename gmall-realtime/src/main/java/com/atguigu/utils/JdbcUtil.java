package com.atguigu.utils;

// 查询 JDBC 工具类,所有涉及 JDBC 连接的都可以用
// 只要是查询 select 语句都可以用

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    // 返回值类型是泛型,返回值前面需要也加个泛型
    // 参数：连接的语句，sql语句，具体封装成什么类型，underScoreToCamel命名方式转换:数据库中表名有 _ ,java命名是小驼峰命名 需要添加依赖
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws Exception {
        // 创建集合用于存放查询结果对象
        ArrayList<T> result = new ArrayList<>();
        // 查询这个数据,将每一行数据都封装成一个T对象
        // 编译SQL语句
        // 生产环境中异常主要是抛的，谁用谁处理这个异常，这个异常是复用的
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        // 执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        // 得到查询的元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        // 列名可以通过 int 值获取，
        int columnCount = metaData.getColumnCount();

        // 遍历查询结果resultSet,对每行数据封装 T 对象,并将 T 对象添加至集合
        while (resultSet.next()) {
            // 构建 T 对象
            T t = clz.newInstance();

            // SQL不确定，不确定有几列，不能用数字写死
            // 在查询结果的返回值中有列信息
            // 对行数进行遍历
            for (int i = 1; i < columnCount + 1; i++) {

                // 获取列名,用于后期进行列名格式转化
                String columnName = metaData.getColumnName(i);

                // 每行数据信息
                Object value = resultSet.getObject(i);

                // 对列名进行判断，看是否需要进行列名格式转换
                if (underScoreToCamel) {
                    // 使用guava包
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());    // 未来连接的数据库不确定,这里统一将列名转换为小写
                }
                // t中没有 set 这些将数据信息和列名放入进去的方法
                // 因此需要用到导入的Bean
                BeanUtils.setProperty(t,columnName,value);
            }
            // 将结果添加到集合中
            result.add(t);
        }

        resultSet.close();
        preparedStatement.close();

        // 返回结果集
        return result;

    }

    // 测试
    public static void main(String[] args) throws Exception{
        // 构建连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        System.out.println(queryList(
                connection,
                "select * from GMALL210625_REALTIME.DIM_BASE_TRADEMARK",
                JSONObject.class,
                true   // false 不进行格式转换,true 进行格式转换

        ));

        connection.close();

    }



}
