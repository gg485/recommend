package recommend.demo.utils;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.http.HttpHost;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class MysqlUtils implements Serializable {
    static class MyForeachFunction implements ForeachPartitionFunction<Row> {
        final MysqlDataSource dataSource;

        public MyForeachFunction(MysqlDataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public void call(Iterator<Row> t) throws Exception {
            RestHighLevelClient cli = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http")));
            Connection c = dataSource.getConnection();
            PreparedStatement statement = c.prepareStatement("insert into movie(movieid, name,types) values (?,?,?)");
            while (t.hasNext()) {
                Row row = t.next();
                Object movieId = row.get(0);
                String movieName = row.getString(1);
                String movieTypes = row.getString(2);
                statement.setInt(1, Integer.parseInt(movieId.toString()));
                statement.setString(2, movieName);
                statement.setString(3, movieTypes);
                statement.execute();
                IndexRequest indexRequest = new IndexRequest("recommend")
                        .id(String.valueOf(movieId))
                        .source("name", movieName)
                        .source("types",movieTypes);
                indexRequest.timeout(TimeValue.timeValueSeconds(1));
                indexRequest.timeout("1s");
                cli.index(indexRequest, RequestOptions.DEFAULT);
            }
        }
    }
    public static void main(String[] args) throws IOException {
        ConcurrentSkipListMap<Integer,Integer>m=new ConcurrentSkipListMap<>();

        SparkSession sess = SparkSession.builder().master("local[4]").enableHiveSupport().getOrCreate();
        sess.sparkContext().getConf().registerKryoClasses(new Class<?>[]{MyForeachFunction.class});
        sess.sparkContext().setLogLevel("ERROR");
        Dataset<Row> movies = sess.read().csv("file:///D:/Desktop/ml-latest/movies.csv");
        movies.write().mode(SaveMode.Overwrite).parquet("/tmp/movies");
        sess.sql("create table if not exists movies(movieId int,title string,genres string) stored as parquet");
        sess.sql("load data inpath \"/tmp/movies\" overwrite into table movies");
        sess.sql("select * from movies limit 10").show();

        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/recommend?characterEncoding=UTF-8");
        dataSource.setUser("root");
        dataSource.setPassword("gddsygy_12345");
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        CreateIndexRequest request = new CreateIndexRequest("recommend");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1)
        );
        Map<String, Object> name = new HashMap<>();
        name.put("type", "text");
        Map<String, Object> types = new HashMap<>();
        name.put("type", "text");
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", name);
        properties.put("types", types);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        request.mapping(mapping);
        client.indices().create(request, RequestOptions.DEFAULT);

        MyForeachFunction func = new MyForeachFunction(dataSource);
        movies.foreachPartition(func);
    }
}
