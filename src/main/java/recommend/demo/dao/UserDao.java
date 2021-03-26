package recommend.demo.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import recommend.demo.dao.mapper.UserMapper;
import recommend.demo.model.User;

@Repository
public class UserDao {
    @Autowired
    UserMapper userMapper;
    public boolean isNewUser(int uid){
        User user = userMapper.getSimpleUser(uid);
        return user==null;
    }
}
