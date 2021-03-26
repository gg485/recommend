package recommend.demo.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import recommend.demo.dao.MovieDao;
import recommend.demo.dao.mapper.MovieMapper;
import recommend.demo.model.Event;
import recommend.demo.model.Movie;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class SearchService {
    public static final String MOVIE_KEY_PRE="movie_";
    @Qualifier("cli")
    @Autowired
    RestHighLevelClient client;
    @Autowired
    KafkaProducer<String, Event> kafkaProducer;
    @Autowired
    MovieMapper movieMapper;
    @Autowired
    Jedis jedis;

    public List<Movie> search(int uid,String name) throws IOException {
        List<Movie> list=new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", name));
        searchRequest.source(searchSourceBuilder);
        final SearchResponse rsp = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = rsp.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String movieName = (String) sourceAsMap.get("name");
            String movieTypes = (String) sourceAsMap.get("types");
            int movieId = Integer.parseInt(hit.getId());
            Movie movie = new Movie(movieId, movieName, movieTypes);
            list.add(movie);
            ProducerRecord<String, Event> record = new ProducerRecord<>("recommend", new Event(uid,movie,1,0, 0));
            kafkaProducer.send(record);
        }
        return list;
    }

    public void recordClick(int uid,int id) {
        Movie movie = movieMapper.getMovieById(id);
        ProducerRecord<String, Event> record = new ProducerRecord<>("recommend", new Event(uid,movie,0,1,0));
        kafkaProducer.send(record);
    }

    public String getImgUrl(int id){
        String key=MOVIE_KEY_PRE+id;
        int tmdbId;
        try {
            tmdbId = Integer.parseInt(jedis.get(key));
        }catch (Exception e){
            return "";
        }
        String url="https://www.themoviedb.org/movie/"+tmdbId;
        Connection conn = Jsoup.connect(url);
        Document doc = null;
        try {
            doc = conn.get();
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert doc != null;
        Elements elements = doc.select(".poster.lazyload");
        for (Element element : elements) {
            String attr = element.attr("data-src");
            System.out.println(attr);
            return "https://www.themoviedb.org"+attr;
        }
        return "";
    }
}
