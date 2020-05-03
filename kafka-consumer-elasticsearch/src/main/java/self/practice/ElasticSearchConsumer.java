package self.practice;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static RestHighLevelClient createClient() {
        String hostName = "";
        String userName = "";
        String password = "";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(hostName, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        }));
        return client;
    }
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        while(true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            int recordCount = consumerRecords.count();
            logger.info("Received "+ recordCount +" records");
            BulkRequest bulkRequest =  new BulkRequest();
            for (ConsumerRecord<String, String> record : consumerRecords){
                /**
                 * two strategies
                 * kafka generic id
                 * String id = record.topic() +"_"+ record.partition()+"_"+record.offset();
                 * String id = extractIdfromTweet(record.value());
                 */
                try {
                    String id = extractIdfromTweet(record.value());
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id /*to make consumer idempotent*/)
                            .source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest); // we add to bulk request
                }catch (NullPointerException e) {
                    logger.warn("Skipping bad record: "+record.value());
                }
                catch (IllegalStateException e) {
                    logger.warn("Not a JSON Object: "+record.value());
                }
                // IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                // logger.info(indexResponse.getId());
            }
            if (recordCount>0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Commiting offsets..!!");
                consumer.commitSync();
                logger.info("Offsets committed!");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdfromTweet(String tweet) {
        return jsonParser.parse(tweet).getAsJsonObject().
                get("id_str").
                getAsString();
    }

    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootStrapServer ="127.0.0.1:9092";
        String groupID = "kafka-demo-elasticsearch";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); //auto commit is enabled by default, disabling it.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
}
