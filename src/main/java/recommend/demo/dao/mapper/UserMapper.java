package recommend.demo.dao.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;
import recommend.demo.model.User;

import java.util.List;

@Repository
@Mapper
public interface UserMapper {
    User getSimpleUser(int uid);
    void saveScore(int uid,int movieId,double rating);
    Double getScore(@Param("movieId") int movieId);
    void saveStar(int uid,int movieId);
}
