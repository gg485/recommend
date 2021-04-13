package recommend.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.core.StringRedisTemplate;
import recommend.demo.service.RealtimeService;
import recommend.demo.utils.BeanUtils;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        System.setProperty("spark.driver.allowMultipleContexts", "true");
        SpringApplication.run(DemoApplication.class, args);
        RealtimeService realtimeService = BeanUtils.getBean("realtimeService");
        if(realtimeService == null){
            throw new NullPointerException("realtimeService is null");
        }
        new Thread(realtimeService.getStreamWorker()).start();
    }
}
