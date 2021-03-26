package recommend.demo.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;

public class ALSHolder {
    private static volatile ALSHolder alsHolder;
    private static MatrixFactorizationModel model;
    private static long lastUpdateTime;
    @Autowired
    SparkSession sess;

    private ALSHolder() {
    }

    public static ALSHolder getInstance() {
        if (alsHolder == null) {
            synchronized (ALSHolder.class) {
                if (alsHolder == null) {
                    alsHolder = new ALSHolder();
                }
            }
        }
        return alsHolder;
    }

    public synchronized MatrixFactorizationModel getModel() {
        if (alsHolder == null) {
            return null;
        }
        if (model != null) {
            long curTime = System.currentTimeMillis();
            if (curTime - lastUpdateTime < 86400 * 1000) {
                return model;
            }
        }
        Dataset<Row> df = sess.sql("select userid,movieid,rating from ratings");
        try {
            df.createGlobalTempView("ratings");
        } catch (AnalysisException ignored){}
        JavaRDD<Rating> ratingRDD = df.javaRDD().map((Function<Row, Rating>) v1 ->
                new Rating(v1.getInt(0), v1.getInt(1), v1.getDouble(2)));
        System.out.println("training begin!");
        model=ALS.train(JavaRDD.toRDD(ratingRDD), 10, 10, 0.01);
        System.out.println("training complete!");
        lastUpdateTime=System.currentTimeMillis();
        return model;
    }
}
