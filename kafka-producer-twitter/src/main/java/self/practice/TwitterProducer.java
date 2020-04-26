package self.practice;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private String consumerKey = "";
    private String consumerSecret = "";
    private String token = "";
    private String secret = "";

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }



    public void run(){
        //create twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createTwitterClient(msgQueue);

        //connect with the client
        client.connect();

        //create kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application..!!");
            logger.info("shutting down client for twitter..!!");
            client.stop();
            logger.info("closing producer..!!");
            producer.close();
            producer.flush();
            logger.info("done.!");
        }));

        //loop to send data to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord("twitter_tweets",null, msg), new Callback(){
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.info("Something unexpected happened..!!",e);
                        }
                    }
                });
            }
        }
        logger.info("End of Application..!!");
    }
    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("sonia gandhi");
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
        BlockingQueue<Event> eventQueue;
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
// Attempts to establish a connection.
       return hosebirdClient;
    }

    public KafkaProducer<String,String> createKafkaProducer() {
        String bootStrapServer ="127.0.0.1:9092";
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);

        //create safe producer
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,5); //use 1 is using kafka below version 1.1

        //high throughput producer properties
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); //32 kb batch size

        //create kafka producer
        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<String, String>(properties);
        return stringStringKafkaProducer;
    }
}
