package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService iSeckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("lua/seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

//    private BlockingQueue<VoucherOrder> orders = new ArrayBlockingQueue<>(1024*1024);
    private static final ExecutorService SINGLE_THREAD_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SINGLE_THREAD_EXECUTOR.submit(new VoucherOrderHandle());
    }

    private class VoucherOrderHandle implements Runnable{
        String queName = "stream.orders";
        @Override
        public void run() {
            while (true){
                try {
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queName, ReadOffset.lastConsumed())
                    );
                    if (read ==null||read.isEmpty()){
                        continue;
                    }
                    MapRecord<String, Object, Object> entries = read.get(0);
                    Map<Object, Object> value = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    stringRedisTemplate.opsForStream().acknowledge(queName,"g1",entries.getId());
                } catch (Exception e) {
                    log.error("创建订单异常");
                    handlePendingList();
                }
            }
        }
        private void handlePendingList() {
            while (true){
                try {
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queName, ReadOffset.from("0"))
                    );
                    if (read ==null||read.isEmpty()){
                        break;
                    }
                    MapRecord<String, Object, Object> entries = read.get(0);
                    Map<Object, Object> value = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    stringRedisTemplate.opsForStream().acknowledge(queName,"g1",entries.getId());
                } catch (Exception e) {
                    log.error("处理pendingList异常");
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {

                    }
                }
            }

        }


    }
//    private class VoucherOrderHandle implements Runnable{
//        @Override
//        public void run() {
//            while (true){
//                try {
//                    VoucherOrder voucherOrder = orders.take();
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("创建订单异常");
//
//                }
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLocked = lock.tryLock();
        if (!isLocked){
            return ;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }
    private  IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        Long execute = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        int i = execute.intValue();
        if (i!= 0 ){
            return Result.fail(i==1 ?"库存不足":"不能重复下单");
        }
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId = UserHolder.getUser().getId();
//        long orderId = redisIdWorker.nextId("order");
//        Long execute = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                userId.toString()
//        );
//        int i = execute.intValue();
//        if (i!= 0 ){
//            return Result.fail(i==1 ?"库存不足":"不能重复下单");
//        }
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setId(orderId);
//        voucherOrder.setVoucherId(voucherId);
//        voucherOrder.setUserId(UserHolder.getUser().getId());
//        //保存到阻塞队列
//        orders.add(voucherOrder);
//        //获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        return Result.ok(orderId);
//    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        SeckillVoucher seckillVoucher = iSeckillVoucherService.getById(voucherId);
//        LocalDateTime beginTime = seckillVoucher.getBeginTime();
//        if (beginTime.isAfter(LocalDateTime.now())) {
//            return Result.fail("尚未开始");
//        }
//        LocalDateTime endTime = seckillVoucher.getEndTime();
//        if (endTime.isBefore(LocalDateTime.now())) {
//            return Result.fail("已经结束");
//        }
//        if (seckillVoucher.getStock()<1){
//            return Result.fail("库存不足");
//        }
//
//        Long id = UserHolder.getUser().getId();
////        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + id, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + id);
//        boolean isLocked = lock.tryLock();
////        boolean isLocked = simpleRedisLock.tryLock(1200);
//        if (!isLocked){
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long id = voucherOrder.getUserId();
        Integer count = query().eq("user_id", id).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count>0){
            log.error("已经购买过");
            return ;
        }
        boolean success = iSeckillVoucherService.update().setSql("stock = stock -1").eq("voucher_id", voucherOrder.getVoucherId()).gt("stock",0).update();
        if(!success){

            log.error("库存不足");
            return;
        }
        save(voucherOrder);
    }


}
