package com.hmdp;

import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class HmDianPingApplicationTests {
    @Autowired
    private RedisIdWorker redisIdWorker;

    private ExecutorService executorService = Executors.newFixedThreadPool(500);
    @Test
    void testIdWork() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(300);
        Runnable task = ()->{
            for (int i = 0; i < 100; i++) {
                long order = redisIdWorker.nextId("order");
                System.out.println("order"+order);
            }
            countDownLatch.countDown();
        };
        long l = System.currentTimeMillis();
        for (int i = 0; i <300 ; i++) {
            executorService.submit(task);
        }
        countDownLatch.await();
        long end = System.currentTimeMillis();
        System.out.println("time="+(end- l));
    }
    
}
