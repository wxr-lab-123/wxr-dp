package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.conditions.update.UpdateChainWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author cyw
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisTemplate redisTemplate;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 秒杀优惠券功能入口
     *
     * @param voucherId 优惠券ID
     * @return 返回秒杀结果，包含订单ID或错误信息
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 查询优惠券信息
        SeckillVoucher sv = seckillVoucherService.getById(voucherId);

        // 判断秒杀时间是否合法
        if (sv.getBeginTime().isAfter(LocalDateTime.now()) || sv.getEndTime().isBefore(LocalDateTime.now()))
            return Result.fail("秒杀尚未开始或已经结束");

        // 判断库存是否充足
        if (sv.getStock() < 1)
            return Result.fail("库存不足");

        // 获取当前用户ID并加锁防止并发下单
        Long userId = UserHolder.getUser().getId();
        // 创建锁对象
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        boolean isLock = simpleRedisLock.tryLock(1200);
        // 获取锁
        if (!isLock) {
            return Result.fail("一人一单这一块");
        }
        // 获取锁成功,创建订单
//        synchronized (userId.toString().intern()) {
//            // 获取当前代理对象以支持事务传播
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoutherOrder(voucherId);
//        }
        try {
            // 获取当前代理对象以支持事务传播
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoutherOrder(voucherId);
        } finally {
            // 释放锁
            simpleRedisLock.unlock();
        }
    }

    /**
     * 创建秒杀订单
     *
     * @param voucherId 优惠券ID
     * @return 返回创建结果，包含订单ID或错误信息
     */
    @Transactional
    public Result createVoutherOrder(Long voucherId) {
        // 防止用户重复下单：查询该用户是否已购买过此优惠券
        Long userId = UserHolder.getUser().getId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();

        // 若已存在订单则返回失败
        if (count > 0) {
            return Result.fail("用户已经购买过一次!!!");
        }

        // 扣减库存（使用乐观锁保证线程安全）
        Boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .gt("stock", 0)
                .eq("voucher_id", voucherId)
                .update();

        // 库存扣减失败返回错误
        if (!success) {
            return Result.fail("库存不足");
        }

        // 构造订单信息并保存
        VoucherOrder voucherOrder = new VoucherOrder();
        UserDTO user = UserHolder.getUser();
        voucherOrder.setUserId(user.getId());
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        return Result.ok(orderId);
    }
}
