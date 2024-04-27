package libtagpropagation;

import com.twitter.chill.protobuf.ProtobufSerializer;
import libtagpropagation.graphalignment.GraphAlignmentProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import provenancegraph.AssociatedEvent;
import provenancegraph.datamodel.PDM;

import provenancegraph.parser.PDMParser;
import utils.KafkaConfig;
import utils.KafkaPDMDeserializer;

import java.util.ArrayList;

public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<AssociatedEvent> event_stream;

            KafkaConfig kafkaConfig = new KafkaConfig();
            String kafkaBroker = kafkaConfig.getKafkaBroker();
            String kafkaGroupId = kafkaConfig.getKafkaGroupID();
//            String kafkaTopic = kafkaConfig.getTopic("Topic-1");
            String kafkaTopic = "topic-THEIA-6r";
//            ArrayList<String> kafkaTopicList = kafkaConfig.getTopicList(1, 1);

            env.getConfig().registerTypeWithKryoSerializer(PDM.LogPack.class, ProtobufSerializer.class);
            KafkaSource<PDM.LogPack> source = KafkaSource.<PDM.LogPack>builder()
                    .setBootstrapServers(kafkaBroker)
                    .setTopics(kafkaTopic)
                    .setGroupId(kafkaGroupId)
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new KafkaPDMDeserializer())
//                    .setProperty("max.poll.records", "100")
                    .build();

            DataStream<PDM.LogPack> logPack_stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
            DataStream<PDM.Log> log_stream = logPack_stream.flatMap(new PDMParser());
            event_stream = log_stream.map(PDMParser::initAssociatedEvent);
//            event_stream.print();

        event_stream.keyBy(associatedEvent -> associatedEvent.hostUUID)
                .process(new GraphAlignmentProcessFunction());

        // Execute the Flink job
        env.execute("Online Flink");
    }
}
