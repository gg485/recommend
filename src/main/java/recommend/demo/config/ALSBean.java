package recommend.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import recommend.demo.utils.ALSHolder;

@Configuration
public class ALSBean {
    @Bean
    public ALSHolder alsHolder(){
        return ALSHolder.getInstance();
    }
}
