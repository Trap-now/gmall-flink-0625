package com.atguigu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection,String table, String key) throws Exception {

        // 查询 Redis 数据
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + table + ":" + key;
        String jsonStr = jedis.get(redisKey);

        // 当第一次查或者以前查过过期了 jsonStr 有可能为 null
        if ( jsonStr != null ) {
            // 重置过期时间,有可能24h一直在访问,当每次访问的时候就重置过期时间
            jedis.expire(redisKey,24 * 60 * 60);
            // 归还连接
            jedis.close();
            // 返回结果数据
            return JSON.parseObject(jsonStr);
        }


        // 构建查询语句
        String querySql = "select * from "+ GmallConfig.HBASE_SCHEMA +"."+ table +" where id='"+key+"'";
        System.out.println("QuerySql:" + querySql);
        // 执行查询
        List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);

        // 当第一次访问时,将数据写入 Redis
        // 获取数据
        JSONObject dimInfo = jsonObjects.get(0);
        // 将数据转换成字符串写入 Redis
        jedis.set(redisKey,dimInfo.toJSONString());
        // 第一次访问,设置过期时间
        jedis.expire(redisKey,24 * 60 * 60);
        // 归还连接
        jedis.close();

        return dimInfo;
    }

    // 删除Redis中的数据
    public static void deleteDimInfo(String table,String key) {

        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + table + ":"  + key;

        jedis.del(redisKey);

        jedis.close();

    }

    public static void main(String[] args) throws Exception{
        // 构建连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        // 获取系统时间查看查询的时间
        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection,"DIM_BASE_TRADEMARK","1"));
        long end = System.currentTimeMillis();
        System.out.println(getDimInfo(connection,"DIM_BASE_TRADEMARK","1"));
        long end2 = System.currentTimeMillis();

        System.out.println(end - start);    // 第一次访问消耗多久时间，最复杂的就是发请求，与第三方交互消耗时间久
        System.out.println(end2 - end);     // 第二次访问消耗多久时间


        connection.close();
    }
}
