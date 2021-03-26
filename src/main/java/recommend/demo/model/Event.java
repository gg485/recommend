package recommend.demo.model;

import java.io.Serializable;

public class Event implements Serializable {
    int uid;
    Movie movie;
    int see;
    int click;
    int star;

    public Event() {
    }

    public Event(int uid, Movie movie, int see, int click, int star) {
        this.uid = uid;
        this.movie = movie;
        this.see = see;
        this.click = click;
        this.star = star;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public Movie getMovie() {
        return movie;
    }

    public void setMovie(Movie movie) {
        this.movie = movie;
    }

    public int getSee() {
        return see;
    }

    public void setSee(int see) {
        this.see = see;
    }

    public int getClick() {
        return click;
    }

    public void setClick(int click) {
        this.click = click;
    }

    public int getStar() {
        return star;
    }

    public void setStar(int star) {
        this.star = star;
    }
}
