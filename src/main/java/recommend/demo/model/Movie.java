package recommend.demo.model;

import java.io.Serializable;

public class Movie implements Serializable {
    int id;
    String title;
    String types;
    String url;

    public Movie() {
    }

    public Movie(int id, String title,String types) {
        this.id = id;
        this.title = title;
        this.types=types;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTypes() {
        return types;
    }

    public void setTypes(String types) {
        this.types = types;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
