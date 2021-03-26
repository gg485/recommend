package recommend.demo.dao;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;
import recommend.demo.dao.mapper.MovieMapper;
import recommend.demo.model.IdRating;
import recommend.demo.model.Movie;
import recommend.demo.utils.ALSHolder;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Repository
public class MovieDao {
    public static final String TOP_MOVIES_KEY = "top_movies";
    public static final String RCM_MOVIES_KEY_PRE = "recommend_movies_";
    @Autowired
    Jedis jedis;
    @Autowired
    SparkSession sess;
    @Autowired
    MovieMapper movieMapper;
    @Autowired
    ALSHolder als;

    public List<Movie> getTopMovies(int n) {
        ObjectMapper mapper = new ObjectMapper();
        List<Movie> ret = new ArrayList<>();
        Boolean hasKey = Optional.ofNullable(jedis.exists(TOP_MOVIES_KEY)).orElse(false);
        if (hasKey) {
            List<String> strings =jedis.lrange(TOP_MOVIES_KEY, 0, n);
            assert strings != null;
            for (String string : strings) {
                byte[] bytes = string.getBytes();
                try {
                    Movie movie = mapper.readValue(bytes, Movie.class);
                    ret.add(movie);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return ret;
        } else {
            Dataset<Row> df = sess.sql(
                    "select movieid,avg(rating) as score from ratings group by movieid order by score desc");
            Dataset<IdRating> idRatingDataset = df.map((MapFunction<Row, IdRating>) value -> {
                IdRating idRating = new IdRating();
                idRating.setId(value.getInt(0));
                idRating.setRating(value.getDouble(1));
                return idRating;
            }, Encoders.bean(IdRating.class));
            Dataset<IdRating> limit = idRatingDataset.limit(n);
            limit.foreachPartition((ForeachPartitionFunction<IdRating>) t -> {
                while (t.hasNext()) {
                    IdRating next = t.next();
                    ret.add(movieMapper.getMovieById(next.getId()));
                }
            });
            for (Movie movie : ret) {
                try {
                    String s = mapper.writeValueAsString(movie);
                    jedis.rpush(TOP_MOVIES_KEY, s);
                    jedis.expire(TOP_MOVIES_KEY, 86400);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return ret;
    }

    public List<Movie> getRecommendMovies(int uid, int n)  {
        ObjectMapper mapper = new ObjectMapper();
        List<Movie> ret = new ArrayList<>();
        String key=RCM_MOVIES_KEY_PRE+uid;
        Boolean hasKey = Optional.ofNullable(jedis.exists(key)).orElse(false);
        if(hasKey){
            List<String> list = jedis.lrange(key, 0, -1);
            assert list != null;
            if(list.size()>=n){
                for(int i=0;i<n;i++){
                    try {
                        ret.add(mapper.readValue(list.get(i).getBytes(),Movie.class));
                    } catch (IOException ignored) {
                    }
                }
                return ret;
            }
            jedis.del(key);
        }
        MatrixFactorizationModel model = als.getModel();
        Rating[] ratings = model.recommendProducts(uid, n);
        for (Rating rating:ratings) {
            Movie movie = movieMapper.getMovieById(rating.product());
            ret.add(movie);
            try {
                String s = mapper.writeValueAsString(movie);
                jedis.rpush(key,s);
            } catch (IOException ignored) {
            }
        }
        return ret;
    }
}
