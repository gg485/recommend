package recommend.demo.service;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import recommend.demo.model.Event;
import recommend.demo.model.Movie;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

@Service("realtimeService")
public class RealtimeService implements Serializable {
    public static final String REALTIME_PRE = "realtime_";
    @Autowired
    transient Jedis jedis;
    @Autowired
    SimilarService similarService;
    @Autowired
    BloomFilter filter;
    @Autowired
    StreamExecutionEnvironment env;
    
    ObjectMapper mapper=new ObjectMapper();

    Runnable streamWorker=new Runnable() {
        @Override
        public void run() {
            Properties kafkaParams = new Properties();
            kafkaParams.put("bootstrap.servers", "localhost:9092");
            //kafkaParams.put("key.deserializer", StringDeserializer.class);
            //kafkaParams.put("value.deserializer", EventDeserializer.class);
            kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", true);

            DataStream<Event> ds = env.addSource(new FlinkKafkaConsumer<>("test", new DeserializationSchema<Event>() {
                @Override
                public Event deserialize(byte[] message) throws IOException {
                    return mapper.readValue(message, Event.class);
                }

                @Override
                public boolean isEndOfStream(Event nextElement) {
                    return false;
                }

                @Override
                public TypeInformation<Event> getProducedType() {
                    return TypeInformation.of(Event.class);
                }
            }, kafkaParams).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Event>() {
                long ts;
                final long BOUND=100L;
                @Override
                public Watermark getCurrentWatermark() {
                    return new Watermark(ts-BOUND); //tolerate 100ms delay
                }

                @Override
                public long extractTimestamp(Event element, long previousElementTimestamp) {
                    ts=previousElementTimestamp;
                    return ts;
                }
            }));
            ds.keyBy(Event::getUid).timeWindow(Time.seconds(600))
                    .process(new ProcessWindowFunction<Event, Void, Integer, TimeWindow>() {
                        @Override
                        public void process(Integer uid, Context context, Iterable<Event> events, Collector<Void> out) throws Exception {
                            Iterator<Event> ite = events.iterator();
                            Map<String,EnumMap<Event.EventType,Integer>>map=new HashMap<>();
                            int count=0;
                            while(ite.hasNext()){
                                Event event = ite.next();
                                for (String t : event.getMovie().getTypes().split("\\|")) {
                                    map.putIfAbsent(t,new EnumMap<>(Event.EventType.class));
                                    map.get(t).compute(event.getType(),(type,c)-> (c==null?0:c)+1);
                                }
                                count++;
                            }
                            if(count<3)return;
                            map.forEach((movieType,types)->{
                                String key = REALTIME_PRE + uid;
                                int sum = types.values().stream().mapToInt(Integer::valueOf).sum();
                                int notInterestCount=types.getOrDefault(Event.EventType.NotInterest,0);
                                if(sum<10&&notInterestCount==0){
                                    return;
                                }
                                double score=0.0;
                                String hget = jedis.hget(key, movieType);
                                if(!"nil".equals(hget))score=Double.parseDouble(hget);
                                if(notInterestCount!=0){
                                    jedis.hset(key,movieType,String.valueOf(score*Math.pow(0.5,notInterestCount)));
                                }else{
                                    int see=types.getOrDefault(Event.EventType.See,0);
                                    int click=types.getOrDefault(Event.EventType.Click,0);
                                    int star=types.getOrDefault(Event.EventType.Star,0);
                                    if(see!=0){
                                        jedis.hset(key,movieType,String.valueOf((double)(click+2*star)/see));
                                    }
                                }
                                filter.add(new Key(String.valueOf(uid).getBytes()));
                            });
                        }
                    }
            );
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    public Runnable getStreamWorker(){
        return streamWorker;
    }

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
        if(filter.membershipTest(new Key(String.valueOf(uid).getBytes()))){
            return new ArrayList<>();
        }
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
