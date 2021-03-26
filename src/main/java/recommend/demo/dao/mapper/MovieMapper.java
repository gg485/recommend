package recommend.demo.dao.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;
import recommend.demo.model.Movie;

@Repository
@Mapper
public interface MovieMapper {
    Movie getMovieById(int id);
}
