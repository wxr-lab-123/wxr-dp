package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author cyw
 * @since 2021-12-22
 */
@Service
@Slf4j
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Resource
    private CacheClient cacheClient;
    /**
     * 根据店铺ID查询店铺信息，支持缓存穿透和缓存击穿处理
     *
     * @param id 店铺ID
     * @return Result 包含店铺信息的结果对象
     */
    @Override
    public Result queryById(Long id) {
        // 实现缓存穿透的方法
        //queryWithPassThrough( id);
        //实现缓存击穿的方法
//        Shop shop = queryWithMutex(id);
//        if (shop == null) {
//            return Result.fail("店铺不存在");
//        }
       //Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY ,id,Shop.class,this::getById,CACHE_SHOP_TTL, TimeUnit.MINUTES);
        Shop shop = cacheClient.queryWithLogicalExpire(
                RedisConstants.CACHE_SHOP_KEY,
                id,
                Shop.class,
                this::getById,
                20L,
                TimeUnit.MINUTES
        );
        return Result.ok(shop);


    }

    /**
     * 使用互斥锁方式解决缓存击穿问题
     *
     * @param id 店铺ID
     * @return Shop 店铺对象，如果不存在则返回null
     */
    private Shop queryWithMutex(Long id) {
        // 1.在redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 2.缓存命中.返回
        if (StrUtil.isNotBlank(shopJson)) {
            log.debug("查询缓存成功");
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        if (shopJson != null) {
            return null;
        }
        // 3.缓存未命中
        // 1.获取互斥锁
        String lock = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lock);
            // 判断是否获取成功
            if (!isLock) {
                // 2.获取锁失败
                // 等待5秒再重试 休眠,加重试
                Thread.sleep(50);
                return queryWithMutex( id);
            }
            // 3.获取锁成功
            // 4.查询数据库
            shop = getById(id);
            // 模拟重建耗时
            Thread.sleep(200);
            if (shop == null) {
                // 缓存空值
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 5.数据库不存在,返回错误
                return null;
            }
            // 6.数据库存在,写入缓存,并设置超时时间
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
            // 释放互斥锁

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(lock);
        }
        // 7.返回
        return shop;

    }

    /**
     * 更新店铺信息，并删除对应的缓存
     *
     * @param shop 店铺对象
     * @return Result 操作结果
     */
    @Override
    @Transactional
    public Result update(Shop shop) {
        updateById( shop);
        if (shop.getId() == null) {
            return Result.fail("店铺不存在");
        }
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());
        log.debug("更新缓存成功");
        return Result.ok();
    }

    /**
     * 使用缓存空值方式解决缓存穿透问题
     *
     * @param id 店铺ID
     * @return Shop 店铺对象，如果不存在则返回null
     */
    public Shop queryWithPassThrough(Long id) {
        // 1.在redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 2.缓存命中.返回
        if (StrUtil.isNotBlank(shopJson)) {
            log.debug("查询缓存成功");
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        if (shopJson != null) {
            return null;
        }
        // 3.缓存未命中
        // 4.查询数据库
        Shop shop = getById(id);
        if (shop == null) {
            // 缓存空值
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 5.数据库不存在,返回错误
            return null;
        }
        // 6.数据库存在,写入缓存,并设置超时时间
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 7.返回
        return shop;

    }

    /**
     * 使用逻辑过期方式解决缓存击穿问题
     *
     * @param id 店铺ID
     * @return Shop 店铺对象，如果不存在则返回null
     */
    private Shop queryWithLogicalExpire(Long id) {
        // 1.在redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        // 2.缓存未命中.返回
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        // 3.缓存命中
        // json -> 对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject jsonData = (JSONObject) redisData.getData();
        Shop shop = jsonData.toBean(Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 缓存未过期.直接返回
            return shop;
        }
        // 缓存已过期.获取锁
        String lock = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lock);

        if (isLock) {
            // 获取成功,创建新的独立线程,重建缓存
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 创建重建缓存
                    saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unLock(lock);
                }
            });
        }
        // 获取锁失败.返回数据
        // 7.返回
        return shop;

    }

    /**
     * 尝试获取分布式锁
     *
     * @param key 锁的键名
     * @return boolean 是否获取成功
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10L, TimeUnit.SECONDS);
        return BooleanUtil.isTrue( flag);
    }

    /**
     * 释放分布式锁
     *
     * @param key 锁的键名
     */
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

    /**
     * 将店铺信息保存到Redis中，并设置逻辑过期时间
     *
     * @param id 店铺ID
     * @param expireSeconds 过期秒数
     * @throws InterruptedException 线程中断异常
     */
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        Shop shop = getById(id);
        Thread.sleep(200);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
}
