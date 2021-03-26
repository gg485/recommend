package recommend.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import recommend.demo.model.Movie;
import recommend.demo.service.HomeService;
import recommend.demo.service.SearchService;

import java.util.List;

@RestController
@CrossOrigin
public class HomeController {
    @Autowired
    HomeService homeService;
    @Autowired
    SearchService searchService;

    @RequestMapping("/recommend")
    public List<Movie> recommend(int uid){
        List<Movie> ret = homeService.homeMovies(uid, 10);
        for(Movie m:ret){
            String url = searchService.getImgUrl(m.getId());
            m.setUrl(url);
        }
        return ret;
    }

}
