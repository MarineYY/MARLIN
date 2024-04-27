package utils;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;

public class KafkaConfig {
    private Properties props;

    public KafkaConfig() throws Exception {
        props = new Properties();
        props.load(new FileInputStream("src/main/resources/kafka.properties"));
    }

    public String getKafkaBroker() {
        return props.getProperty("kafkaBroker");
    }

    public String getKafkaGroupID() {
        return props.getProperty("kafkaGroupId");
    }

    public ArrayList<String> getTopicList(Integer start, Integer end){
        ArrayList<String> topicList = new ArrayList<>();
        for(int i = start; i <= end; i ++){
            topicList.add(props.getProperty("Topic-" + i));
        }
        return topicList;
    }

    public String getTopic(String topic){
        return props.getProperty(topic);
    }
}
