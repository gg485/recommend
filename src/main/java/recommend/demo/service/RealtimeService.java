package recommend.demo.service;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import recommend.demo.model.Event;
import recommend.demo.model.Movie;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Service("realtimeService")
public class RealtimeService implements Serializable {
    public static final String REALTIME_PRE = "realtime_";
    @Autowired
    transient Jedis jedis;
    @Autowired
    SimilarService similarService;

   /* @Override
    public void run() {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("streaming");
        sess.sparkContext();

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        jsc.checkpoint("file:///D:/Desktop/stream/cp");
        jsc.sparkContext().setLogLevel("WARN");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", EventDeserializer.class);
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
    }*/

    public static class EventDeserializer implements Deserializer<Event> {

        @Override
        public Event deserialize(String topic, byte[] data) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                return mapper.readValue(data, Event.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public List<Movie> getRealtimeRcm(int uid, int n) {
        String key = REALTIME_PRE + uid;
        Boolean exists = jedis.exists(key);
        if(!exists){
            return new ArrayList<>();
        }
        Map<String, String> hgetAll = jedis.hgetAll(key);
        String[] strings = hgetAll.keySet().stream().sorted(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return (int) (Double.parseDouble(hgetAll.get(o2)) - Double.parseDouble(hgetAll.get(o1)));
            }
        }).limit(n).toArray(String[]::new);
        Movie mock = new Movie(-1, "", String.join("|", strings));
        return similarService.getSimilar(mock, n);
    }
}
