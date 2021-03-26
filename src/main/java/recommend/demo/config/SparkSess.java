package recommend.demo.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkSess {
    @Bean
    public SparkSession sparkSession(){
        return SparkSession.builder().appName("hive_test")
                .master("local[4]")
                .config("spark.sql.warehouse.dir","/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
    }
}
