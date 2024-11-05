package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IBlogService blogService;
    @Resource
    private IUserService userService;

    @Resource
    private IFollowService followService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if (blog == null){
            return Result.fail("笔记不存在");
        }
        extracted(blog);
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if (user == null){
            return;
        }
        Long userId = user.getId();
        String key = "blog:liked:"+blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score!=null);
    }

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            extracted(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
        Long userId = UserHolder.getUser().getId();
        String key = "blog:liked:"+id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null){
            boolean isSuccess = update().setSql("liked = liked+1 ").eq("id", id).update();
            if (isSuccess){
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }else {
            boolean isSuccess = update().setSql("liked = liked-1 ").eq("id", id).update();
            if (isSuccess){
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
        }

        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {

        String blogId = BLOG_LIKED_KEY+ id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(blogId, 0, 4);
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String join = StrUtil.join(",", ids);
        List<UserDTO> userDTOList = userService.query().in("id",ids).last("order by field (id,"+ join +")").list()
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(userDTOList);

    }

    @Override
    public Result saveBlog(Blog blog) {
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = blogService.save(blog);
        if (!isSuccess){
            return Result.fail("笔记保存失败");
        }
        // 查询笔记作者的所有粉丝
        List<Follow> follows = followService.list(new QueryWrapper<Follow>().eq("follow_user_id", user.getId()));
//        List<Follow> follows = followService.list(new LambdaQueryWrapper<Follow>()
//                .eq(Follow::getFollowUserId, user.getId()));
        // 将笔记推送给所有的粉丝
        for (Follow follow : follows) {
            // 获取粉丝的id
            Long id = follow.getUserId();
            // 推送笔记
            String key = FEED_KEY + id;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }

        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        String key = FEED_KEY +userId;

        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        if (typedTuples==null|typedTuples.isEmpty()){
            return null;
        }
        ArrayList<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            ids.add(Long.valueOf(typedTuple.getValue()));
            long time = typedTuple.getScore().longValue();
            if (time == minTime){
                os++;
            }else {
                minTime = time;
                os =1;
            }

        }
        String join = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("order by field (id," + join + ")").list();
        for (Blog blog : blogs) {
            // 查询blog有关的用户
            extracted(blog);
            isBlogLiked(blog);
        }

        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setOffset(os);
        scrollResult.setMinTime(minTime);
        return Result.ok(scrollResult);
    }

    private void extracted(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

}
