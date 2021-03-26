package recommend.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.Jedis;

@Configuration
public class JedisBean {
    @Bean
    Jedis jedis(){
        return new Jedis("localhost");
    }
}
