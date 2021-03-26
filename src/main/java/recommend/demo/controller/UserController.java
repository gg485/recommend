package recommend.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import recommend.demo.service.UserService;

@RestController
@CrossOrigin
public class UserController {
    @Autowired
    UserService userService;
    @PostMapping("/save_score")
    public void saveScore(int uid, int movieId, double rating){
        userService.saveScore(uid,movieId,rating);
    }
    @PostMapping("/save_star")
    public void saveStar(int uid, int movieId){
        userService.saveStar(uid,movieId);
    }
    @GetMapping("/get_score")
    public double getScore(int movieId){
        return userService.getScore(movieId);
    }
}
