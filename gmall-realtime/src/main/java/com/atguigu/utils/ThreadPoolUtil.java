package com.atguigu.utils;

import lombok.SneakyThrows;
import org.apache.hadoop.util.ThreadUtil;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    // 线程池,用于执行多线程操作
    // 封装成一个单例的模式
    // 使用懒汉模式,使用双重校验(加锁)解决线程安全问题
    private static ThreadPoolExecutor threadPoolExecutor = null;

    // 定义空参的私有化构造方法
    private ThreadPoolUtil( ) {
    }

    // 使用静态方法创建对象
    public static ThreadPoolExecutor getThreadPoolExecutor() {

        // 双重校验
        if (threadPoolExecutor == null) {
            // 加锁
            synchronized (ThreadUtil.class) {
                if (threadPoolExecutor == null) {
                    // 公司中需要做压测来判断如何给定数字
                    threadPoolExecutor = new ThreadPoolExecutor(
                            4,
                            20,
                            60,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>()     // 工作队列,当4个线程用完后用工作队列,工作队列满了后用20

                    );
                }
            }
        }


        return threadPoolExecutor;
    }

    public static void main(String[] args) {

        ThreadPoolExecutor threadPoolExecutor = getThreadPoolExecutor();

        for (int i = 0; i < 10; i++) {
            threadPoolExecutor.execute(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                    Thread.sleep(2000);
                }
            });
        }
    }

}
