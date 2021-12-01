package com.atguigu.bean;

// 注解有其作用域,到时候是写到类上的还是方法上还是属性上,因此需要做一个声明 @Target() ,括号中定义某一个属性不写出,引用的是枚举
// 注解一般都是空的,在用到这个属性的时候会尝试去获取有没有这个注解,如果有,则执行注解的相关内容

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)  // 未来在字段上使用
@Retention(RetentionPolicy.RUNTIME)     // 指定注解生效时间   这里的注解叫 元注解 是注解的注解,类似元数据
public @interface TransientSink {

}
