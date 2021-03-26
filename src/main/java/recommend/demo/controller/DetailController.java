package recommend.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import recommend.demo.dao.mapper.MovieMapper;
import recommend.demo.model.Movie;
import recommend.demo.service.RealtimeService;
import recommend.demo.service.SearchService;
import recommend.demo.service.SimilarService;

import java.util.ArrayList;
import java.util.List;

@RestController
@CrossOrigin
public class DetailController {
    @Autowired
    RealtimeService realtimeService;
    @Autowired
    SimilarService similarService;
    @Autowired
    SearchService searchService;
    @Autowired
    MovieMapper movieMapper;

    @RequestMapping("/realtime")
    public List<Movie>realtime(int uid,int n){
       /* List<Movie>ret=new ArrayList<>();
        ret.add(new Movie(1,"title1","type1|type2"));
        ret.add(new Movie(2,"title2","type3"));*/
        List<Movie> ret = realtimeService.getRealtimeRcm(uid, n);
        for(Movie m:ret){
            String url = searchService.getImgUrl(m.getId());
            m.setUrl(url);
        }
        return ret;
    }
    @RequestMapping("/similar")
    public List<Movie>similar(int mid,int n){
        List<Movie> ret = similarService.getSimilar(movieMapper.getMovieById(mid), n);
        for(Movie m:ret){
            String url = searchService.getImgUrl(m.getId());
            m.setUrl(url);
        }
        return ret;
    }
}
