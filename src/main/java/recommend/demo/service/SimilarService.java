package recommend.demo.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import recommend.demo.dao.mapper.MovieMapper;
import recommend.demo.model.Movie;
import recommend.demo.utils.KDTree;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;

@Service
public class SimilarService {
    @Autowired
    MovieMapper movieMapper;
    @Autowired
    Jedis jedis;

    public static final Map<String, Integer> type2idx = new HashMap<>();
    private static final KDTree kdTree = new KDTree();
    private static KDTree.Node root;

    static {
        type2idx.put("Action", 0);
        type2idx.put("Adventure", 1);
        type2idx.put("Animation", 2);
        type2idx.put("Children's", 3);
        type2idx.put("Comedy", 4);
        type2idx.put("Crime", 5);
        type2idx.put("Documentary", 6);
        type2idx.put("Drama", 7);
        type2idx.put("Fantasy", 8);
        type2idx.put("Film-Noir", 9);
        type2idx.put("Horror", 10);
        type2idx.put("Musical", 11);
        type2idx.put("Mystery", 12);
        type2idx.put("Romance", 13);
        type2idx.put("Sci-Fi", 14);
        type2idx.put("Thriller", 15);
        type2idx.put("War", 16);
        type2idx.put("Western", 17);
    }

    public List<Movie> getSimilar(Movie movie, int n) {
        if (root == null) {
            synchronized (SimilarService.class) {
                List<KDTree.Node> nodeList = new ArrayList<>();
                int cursor = 0;
                ScanResult<String> sr;
                do {
                    sr = jedis.scan(String.valueOf(cursor), new ScanParams().match("type_*").count(1000));
                    List<String> res = sr.getResult();
                    res.forEach(s -> {
                        String[] weights = jedis.get(s).split(",");
                        double[] weight = new double[type2idx.size()];
                        for (int i = 0; i < weights.length; i++) {
                            weight[i] = Double.parseDouble(weights[i]);
                        }
                        KDTree.Node node = new KDTree.Node(Integer.parseInt(s.substring(5)), weight);
                        nodeList.add(node);
                    });
                    cursor = Integer.parseInt(sr.getCursor());
                } while (!sr.isCompleteIteration());
                root = kdTree.build(nodeList, 0);
            }
        }

        String[] types = movie.getTypes().split("\\|");
        double[] weight = new double[type2idx.size()];
        if (types.length == 0) Arrays.fill(weight, 0.1);
        else {
            double w = 1.0;
            for (String type : types) {
                if (!type2idx.containsKey(type)) continue;
                weight[type2idx.get(type)] = w;
                w *= 0.75;
            }
        }
        List<KDTree.Node> nodes = kdTree.searchKNN(root, new KDTree.Node(-1, weight), n + 1);
        nodes.remove(0);
        List<Movie> ret = new ArrayList<>();
        for (KDTree.Node node : nodes) {
            ret.add(movieMapper.getMovieById(node.movieId));
        }
        return ret;
    }
}
