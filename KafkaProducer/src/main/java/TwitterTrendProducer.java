
import twitter4j.Location;
import twitter4j.ResponseList;
import twitter4j.Trends;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class TwitterTrendProducer extends Thread {
    private final static String TOPIC = "new_trends";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092,localhost:9093,localhost:9094";

    private static Producer<Integer, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    StringSerializer.class.getName());
        return new KafkaProducer<Integer, String>(props);
    }


    public Trends read() throws Exception {

        AccessToken accessToken = new AccessToken("3161321025-FajWBXoXT4PAZGNyta38w4krEsK8fp2d5opwdN4",
"p2BPY6BzUKyQJRmIMDUvAwPXjBCksRTBI12sYi5xdkvrC");
        Twitter twitter = new TwitterFactory().getInstance();
        twitter.setOAuthConsumer("5VS16zsltlmES0keEq860Xkut", "m6MFR7FTxn2ON3NrMFALD48DFy8ngVxO1KXfE8g1qhaergxpp2");
        twitter.setOAuthAccessToken(accessToken);
        ResponseList<Location> locations;
        locations = twitter.getAvailableTrends();
        System.out.println("Showing available trends");
        int countWoeid = 0;
        int array[] = new int[1000];
        for (Location location : locations) {
            System.out.println(location.getName() + " (woeid:" + location.getWoeid() + ")");
            array[countWoeid] = location.getWoeid();
            countWoeid++;
        }
        int countTrends = 0;
        Producer<Integer, String> producer = createProducer();
        Trends trendsRet = twitter.getPlaceTrends(2295414);
        for (int j = 0; j < 468; j++) {

            Trends trends = twitter.getPlaceTrends(array[j]);
            long time = System.currentTimeMillis();

            for (Integer i = 0; i < trends.getTrends().length; i++) {
                String trend = trends.getTrends()[i].getName();
                final ProducerRecord<Integer, String> record = new ProducerRecord<Integer,String>(TOPIC, i,trend);

                RecordMetadata metadata = producer.send(record).get();

                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
               
                
                System.out.println("Current location number out of 467 = " + j);
                System.out.println("Total number of trends extracted till now = " + countTrends);
                countTrends++;
                if (countTrends % 1000 == 0)
                    Thread.sleep(900000);// Sleep for 15 minutes after extracting every 1000 trends
            }
        }
        System.out.println("Total number of locations = " + countWoeid);
        System.out.println("Total number of trends till now = " + countTrends);
        return trendsRet;
    }

    public static void main(String[] args) throws Exception {
        TwitterTrendProducer obj = new TwitterTrendProducer();
        Trends trend = obj.read();
    }
}