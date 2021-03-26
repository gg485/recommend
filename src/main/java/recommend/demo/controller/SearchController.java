package recommend.demo.controller;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import recommend.demo.model.Movie;
import recommend.demo.service.SearchService;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

@RestController
@CrossOrigin
public class SearchController {
    @Autowired
    SearchService searchService;
    @RequestMapping("/search")
    public List<Movie> search(int uid,String name) throws IOException {
        List<Movie> ret = searchService.search(uid, name);
        for(Movie m:ret){
            String url = searchService.getImgUrl(m.getId());
            m.setUrl(url);
        }
        return ret;
    }

    @GetMapping("/movie/{id}")
    public void clickMovie(int uid,@PathVariable int id){
        searchService.recordClick(uid,id);
    }
}
