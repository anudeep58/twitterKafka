package com.selfpractice;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweets {
    private static JsonParser jsonParser = new JsonParser();
    private static int extractUserFollowerInTweet(String tweet) {
        try {
            return jsonParser.parse(tweet).getAsJsonObject().
                    get("user").
                    getAsJsonObject().
                    get("followers_count").
                    getAsInt();
        }
        catch (NullPointerException e){
            return 0;
        }
        }

    public static void main(String[] args) {
    //create properties
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

    //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
    // input topic
        KStream<Object, Object> input_topic = streamsBuilder.stream("twitter_tweets");
        KStream<Object, Object> filteredStream = input_topic.filter(
                (k, jsonTweet) -> extractUserFollowerInTweet((String) jsonTweet) > 10000
                //filter for tweets which has user of over 10000 follower
        );
        filteredStream.to("important_tweets");

        //build a topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        //start our streams
        kafkaStreams.start();
    }
}
