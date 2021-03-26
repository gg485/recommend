package recommend.demo.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import recommend.demo.model.Event;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;

public class RealtimeProc {
    public static final String REALTIME_PRE = "realtime_";
    transient static Jedis jedis=new Jedis("localhost");

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("streaming");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        jsc.checkpoint("file:///D:/Desktop/stream/cp");
        jsc.sparkContext().setLogLevel("WARN");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", RealtimeService.EventDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Collections.singletonList("test");


        JavaInputDStream<ConsumerRecord<String, Event>> stream =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );
        stream.map(ConsumerRecord::value).mapToPair((PairFunction<Event, Integer, Event>) event ->
                new Tuple2<>(event.getUid(), event))
                .reduceByKeyAndWindow((Function2<Event, Event, Event>) (v1, v2) ->
                                new Event(v1.getUid(), v1.getMovie(),
                                        v1.getSee() + v2.getSee(),
                                        v1.getClick() + v2.getClick(),
                                        v1.getStar() + v2.getStar())
                        , Durations.seconds(60))
                .foreachRDD((VoidFunction<JavaPairRDD<Integer, Event>>) rdd ->
                        rdd.foreachPartition((VoidFunction<Iterator<Tuple2<Integer, Event>>>) ite -> {
                                    while (ite.hasNext()) {
                                        Tuple2<Integer, Event> next = ite.next();
                                        Integer uid = next._1;
                                        Event event = next._2;
                                        String key = REALTIME_PRE + uid;
                                        for (String type : event.getMovie().getTypes().split("\\|")) {
                                            jedis.hset(key, type, String.valueOf(
                                                    (double) (event.getClick() + 2 * event.getStar()) / event.getSee()));
                                        }
                                    }
                                }
                        )
                );
    }
}
