package com.atguigu.Test;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class testTry {
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

        // 遍历查询结果resultSet,对每行数据封装 T 对象,并将 T 对象添加至集合

        while (resultSet.next()) {

            T t = clz.newInstance();

            try {
                for (int i = 1; i < i+1; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = resultSet.getObject(i);
                    BeanUtils.setProperty(t,columnName,value);
                    result.add(t);
                }
//                if (underScoreToCamel) {
//                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
//                }
            } catch (Exception e) {
                continue;
            }
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


