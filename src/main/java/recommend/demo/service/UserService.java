package recommend.demo.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import recommend.demo.dao.mapper.MovieMapper;
import recommend.demo.dao.mapper.UserMapper;
import recommend.demo.model.Event;
import recommend.demo.model.Movie;

@Service
public class UserService {
    @Autowired
    UserMapper userMapper;
    @Autowired
    MovieMapper movieMapper;
    @Autowired
    KafkaProducer<String, Event> kafkaProducer;

    public void saveScore(int uid,int movieId,double rating){
        userMapper.saveScore(uid,movieId,rating);
    }
    public void saveStar(int uid,int movieId){
        userMapper.saveStar(uid,movieId);
        Movie movie = movieMapper.getMovieById(movieId);
        ProducerRecord<String, Event> record = new ProducerRecord<>("recommend", new Event(uid,movie,0,0,1));
        kafkaProducer.send(record);
    }
    public double getScore(int movieId){
        /*List<Double> list = userMapper.getScore(movieId);
        System.out.println(list);
        OptionalDouble average = list.stream().mapToDouble(Double::valueOf).average();
        return average.orElse(0);*/
        System.out.println(userMapper.getScore(movieId));
        return userMapper.getScore(movieId);
    }
}
