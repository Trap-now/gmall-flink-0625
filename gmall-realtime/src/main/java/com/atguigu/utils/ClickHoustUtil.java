package com.atguigu.utils;


import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHoustUtil {
    // 方法的返回值是JdbcSink调用的 sink() 方法的返回值 SinkFunction
    // SinkFunction<>的<>中,按照调用应该写 VistorStats 但是工具类这里要写 T ,否则就是去了工具类的作用
    // 所以这里泛型的类型应该写 T ,同时要在方法返回值之前做方法类型声明

    // 参数传未来四张表不一样的参数
    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        // preparedStatement 用自己的构建的连接去预编译与sql产生关联
                        // 将 t 中的12个地段给 preparedStatement
                        // 因为字段个数与javabean中声明的属性个数一样,因此可以将 t 中的值遍历出来,按顺序给 ? 赋值即可
                        // 这是 T 类型的对象,获取字段值或字段名用 反射 ,因此这里需要先获取字段名,然后通过字段名调用对象获取属性

                        Class<?> clz = t.getClass();
                        // 通过反射的方式拿到类中的所有属性名和方法名
                        //Field[] fields = clz.getFields();       // 获取所有公有的属性
                        Field[] declaredFields = clz.getDeclaredFields();       // 获取所有的属性
                        // 这里使用 fori 进行遍历,因为 prepareStatement 赋值的时候需要一个 index,如果使用增强for循环不知道第几个属性
                        // 获取所有的属性名

                        int offset = 0;

                        for (int i = 0; i < declaredFields.length; i++) {
                            // 获取当前属性
                            Field field = declaredFields[i];

                            // 通过反射的方法获取当前属性值,这里直接get获取不到,会报编译时异常,因此需设置当前属性可访问
                            field.setAccessible(true);

                            // 尝试获取字段上的注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            // 这个注解有的话,这个对象就不为 null 没有就是 null ,不等于 null 说明这个字段不要,就可以不走赋值的逻辑
                            if (annotation != null) {
                                offset++;
                                continue;
                            }

                            Object value = field.get(t);

                            // 给预编译 SQL 对象赋值
                            // 这里不确定赋值的类型,所以使用 Object 进行代替
                            preparedStatement.setObject(i + 1 - offset, value);

                            /*
                             其他需求：在计算的时候,有可能在Javabean中加入辅助字段,算一些指标的时候不太好算
                             加入一些辅助字段辅助计算,辅助字段是 不需要写出的
                             所以javabean中的字段可能要比 clickhouse 中的字段多,此时遍历javabean遍历次数多于clickhouse中字段数
                             字段对应不上,对于这种字段,就不写出去,所以在赋值的时候进行一些加工
                             javabean中有一类注解 @Transient ,给 bean 对象使用,加了这个注解的字段在序列化的时候这个字段不进行序列化,即不写出去
                             因为这个字段可能是另一个字段的延伸字段,例如：B是A的衍生字段,通过A可以创建B,B就不用非要写出去,此时对B字段使用注解即可

                             这里不是序列化,只是模仿这个注解的方式来写一个注解用于本次需求
                              */

                        }
                        Method[] methods = clz.getMethods();
                    }
                },
                // 刚写完时,类下面报红线表示该类中的构造方法被私有化,应该通过调用 Builder().build() 实现
                new JdbcExecutionOptions.Builder()
                        // 填写一些批处理的参数
                        .withBatchSize(5)   // 5 条数据提交一次,为了时效性,这个批处理的提交应该越小越好
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        // 填写一些连接参数
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)   // 驱动
                        // .withUsername()  // 用不到
                        // .withPassword()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)    // 连接地址
                        .build()
        );

    }
}
