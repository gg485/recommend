package recommend.demo.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import recommend.demo.dao.MovieDao;
import recommend.demo.dao.UserDao;
import recommend.demo.model.Event;
import recommend.demo.model.Movie;

import java.util.List;

@Service
public class HomeService {
    @Autowired
    KafkaProducer<String, Event> kafkaProducer;
    @Autowired
    MovieDao movieDao;
    @Autowired
    UserDao userDao;

    public List<Movie> homeMovies(int uid, int n) {
        List<Movie> ret;
        if (userDao.isNewUser(uid)) {
            ret = movieDao.getTopMovies(n);
        } else {
            ret = movieDao.getRecommendMovies(uid, n);
        }
        for (Movie movie : ret) {
            ProducerRecord<String, Event> record = new ProducerRecord<>("recommend", new Event(uid,movie,1,0,0));
            kafkaProducer.send(record);
        }
        return ret;
    }
}
