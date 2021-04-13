package recommend.demo.config;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BloomFilterBean {
    @Bean
    public BloomFilter bloomFilter(){
        return new BloomFilter(10000,10,1);
    }
}
