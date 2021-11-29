package com.atguigu.app.fun;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimJoinFunction<T> {
    // 定义一个接口,将DimAsyncFunction中的抽象方法和实现方法放在接口中,提高复用性

     String getKey(T input);

     void join(T input, JSONObject dimInfo) throws ParseException;

}
