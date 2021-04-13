package recommend.demo.model;

import java.io.Serializable;
import java.util.EnumMap;
import java.util.EnumSet;

public class Event implements Serializable {
    int uid;
    Movie movie;
    EventType type;

    public Event() {
    }

    public Event(int uid, Movie movie, EventType type) {
        this.uid = uid;
        this.movie = movie;
        this.type = type;
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

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public enum EventType{
        See,Click,Star,NotInterest;
    }
}
