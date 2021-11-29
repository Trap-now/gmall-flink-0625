package com.atguigu.app.fun;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

// 使用泛型类型定义,这样所有的维度都可以使用了
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName=tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        // 连接池
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }


    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        // 该方法被异步调用,来个数据不会阻塞,来了就调用来了就调用
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {

                String key = getKey(input);

                try {
                    // 读取维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);

                    // 将维度信息补充至数据
                    if (dimInfo != null) {
                        // 依旧不能写死,使用抽象方法
                        join(input,dimInfo);
                    }

                    // 将补充完成的数据写出
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }



    // 超时重写方法,当超过指定时间数据会传到该方法中进行处理
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
