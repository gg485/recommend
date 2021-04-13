package recommend.demo.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import recommend.demo.model.Event;

import java.io.IOException;
import java.util.Properties;

@Configuration
public class KafkaProd {
    @Bean
    public KafkaProducer<String, Event>kafkaProducer(){//key:电影id，value:点击(1)or曝光(0)
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        return new KafkaProducer<>(properties,new StringSerializer(),new MyEventSerializer<>());
    }
}
class MyEventSerializer<T> implements Serializer<T> {
    ObjectMapper mapper=new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[]{};
    }
}
class MyDeserializer<T> implements Deserializer<T>{
    Class<T> clazz;
    ObjectMapper mapper=new ObjectMapper();
    public MyDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data,clazz);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}