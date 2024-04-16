package libtagpropagation;

import com.twitter.chill.protobuf.ProtobufSerializer;
import libtagpropagation.graphalignment.GraphAlignmentProcessFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import provenancegraph.AssociatedEvent;
import provenancegraph.datamodel.PDM;
import provenancegraph.parser.LocalParser;
import provenancegraph.parser.PDMParser;
import utils.KafkaPDMDeserializer;

import java.util.Objects;

public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<AssociatedEvent> event_stream;

        if (Objects.equals(args[0], "online")){
            String kafkaBroker = "192.168.10.110:9092";
//            String kafkaTopic = "topic-CADETS-0";
//            String kafkaTopic = "topic-CADETS-1";
//            String kafkaTopic = "topic-CADETS-2";
//            String kafkaTopic = "topic-FD-2";
//            String kafkaTopic = "topic-FD-3";
//            String kafkaTopic = "topic-THEIA-1r";
//            String kafkaTopic = "topic-THEIA-3";
//            String kafkaTopic = "topic-THEIA-5m"   ;
//            String kafkaTopic = "topic-THEIA-6r";
            String kafkaTopic = "topic-win10-malicious";
//            String kafkaTopic = "topic-win10";
//            String kafkaTopic = "topic-winserver-malicious";
//            String kafkaTopic = "topic-winserver";
//            String kafkaTopic = "topic-ubantu-malicious";
//            String kafkaTopic = "topic-ubantu";
//            String kafkaTopic = "topic-test";

            String kafkaGroupId = "mergeAlert";

            // TODO: replace deserializer with protobuf PDM deserializer
            env.getConfig().registerTypeWithKryoSerializer(PDM.LogPack.class, ProtobufSerializer.class);
            KafkaSource<PDM.LogPack> source = KafkaSource.<PDM.LogPack>builder()
                    .setBootstrapServers(kafkaBroker)
                    .setTopics(kafkaTopic)
                    .setGroupId(kafkaGroupId)
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new KafkaPDMDeserializer())
                    .build();

            DataStream<PDM.LogPack> logPack_stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
            DataStream<PDM.Log> log_stream = logPack_stream.flatMap(new PDMParser());
            event_stream = log_stream.map(PDMParser::initAssociatedEvent);
//            event_stream.print();

        }
        else {
            final String inputDirectory = "SystemLog/apt.log";
            final FileSource<String> source =
                    FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(inputDirectory) ).build();
            final DataStream<String> stream =
                    env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
            final DataStream<JsonElement> json_stream = stream.map(Main::convertToJson).name("json-source");
            event_stream = json_stream.map(LocalParser::initAssociatedEvent);
        }

        event_stream.keyBy(associatedEvent -> associatedEvent.hostUUID)
                .process(new GraphAlignmentProcessFunction());

        // Execute the Flink job
        env.execute("Online Flink");
    }

    private static JsonElement convertToJson(String inputLine) {
        Gson gson = new Gson();

        try {
            JsonElement jsonElement = gson.fromJson(inputLine, JsonElement.class);
            System.out.println(gson.toJson(jsonElement));
            return jsonElement;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
