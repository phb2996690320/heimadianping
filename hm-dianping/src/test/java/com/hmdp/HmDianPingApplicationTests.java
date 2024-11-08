package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.service.IShopService;
import com.hmdp.service.IShopTypeService;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {
    @Autowired
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IShopService iShopService;
    @Resource
    private IShopTypeService iShopTypeService;

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

    @Test
    void testRedis() {
        List<Shop> list = iShopService.list();
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(shop -> shop.getTypeId()));
        //1.查询店铺信息
        map.forEach((k,v)->{
            Long typeid  = k;
            String key = SHOP_GEO_KEY+typeid;
            List<Shop> shops = v;
            shops.forEach(shop->{
                double x = shop.getX();
                double y = shop.getY();
                stringRedisTemplate.opsForGeo().add(key,new Point(x,y),shop.getId().toString());
            });
        });
    }


}
