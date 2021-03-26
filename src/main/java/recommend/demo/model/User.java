package recommend.demo.model;

import java.util.List;

public class User {
    Integer uid;
    List<Movie>scored;

    public User() {
    }

    public User(Integer uid, List<Movie> scored) {
        this.uid = uid;
        this.scored = scored;
    }

    public Integer getUid() {
        return uid;
    }

    public void setUid(Integer uid) {
        this.uid = uid;
    }

    public List<Movie> getScored() {
        return scored;
    }

    public void setScored(List<Movie> scored) {
        this.scored = scored;
    }
}
