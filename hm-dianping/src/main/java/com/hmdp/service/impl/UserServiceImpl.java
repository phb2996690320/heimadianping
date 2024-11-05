package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {

        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误");
        }

        //2.生成一个验证码
        String code = RandomUtil.randomNumbers(6);
        System.out.println("验证码为："+code);
        //3.保存
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code,LOGIN_CODE_TTL,TimeUnit.MINUTES);
//        session.setAttribute("code",s);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("失败");
        }

//        Object cacheCode = session.getAttribute("code");
        Object cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY+phone);
//        String code = loginForm.getCode();
//        if (cacheCode==null||!cacheCode.equals(code)){
//            return Result.fail("失败2");
//        }
        User user = query().eq("phone", phone).one();
        if (user ==null){
            user=creatUserWithPhone(phone);
        }
        String token = UUID.randomUUID().toString(true);
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> stringObjectMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fildName,fildValues)->fildValues.toString())
                );
        String key = LOGIN_USER_KEY+token;
        stringRedisTemplate.opsForHash().putAll(key,stringObjectMap);
//        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        stringRedisTemplate.expire(key,LOGIN_USER_TTL,TimeUnit.MINUTES);
        return Result.ok(token);
    }

    private User creatUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName("UserName:"+RandomUtil.randomString(5));
        save(user);
        return user;
    }
}
