package libtagpropagation;

import com.twitter.chill.protobuf.ProtobufSerializer;
import libtagpropagation.graphalignment.GraphAlignmentProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import provenancegraph.AssociatedEvent;
import provenancegraph.datamodel.PDM;

import provenancegraph.parser.PDMParser;
import utils.KafkaConfig;
import utils.KafkaPDMDeserializer;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;

public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<AssociatedEvent> event_stream;

        KafkaConfig kafkaConfig = new KafkaConfig();
        String kafkaBroker = kafkaConfig.getKafkaBroker();
        String kafkaGroupId = kafkaConfig.getKafkaGroupID();
        String kafkaTopicList = kafkaConfig.getTopic("Topic11");
//        ArrayList<String> kafkaTopicList = kafkaConfig.getTopicList(1, 3);

        env.getConfig().registerTypeWithKryoSerializer(PDM.LogPack.class, ProtobufSerializer.class);
        KafkaSource<PDM.LogPack> source = KafkaSource.<PDM.LogPack>builder()
                .setBootstrapServers(kafkaBroker)
                .setTopics(kafkaTopicList)
                .setGroupId(kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new KafkaPDMDeserializer())
                .build();

        DataStream<PDM.LogPack> logPack_stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(6);
//        DataStream<PDM.LogPack> logPack_stream2 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(6);
//        DataStream<PDM.LogPack> logPack_stream3 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(6);
//        DataStream<PDM.LogPack> logPack_stream4 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(6);
//        DataStream<PDM.LogPack> logPack_stream5 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(6);
//        DataStream<PDM.LogPack> logPack_stream6 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(6);
//        DataStream<PDM.LogPack> logPack_stream7 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(6);
//        DataStream<PDM.LogPack> logPack_stream8 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(6);
//        DataStream<PDM.LogPack> logPack_stream9 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(6);
//        DataStream<PDM.LogPack> logPack_stream10 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(6);

        DataStream<PDM.Log> log_stream = logPack_stream.flatMap(new PDMParser()).setParallelism(6);
//        DataStream<PDM.Log> log_stream2 = logPack_stream2.flatMap(new PDMParser()).setParallelism(6);
//        DataStream<PDM.Log> log_stream3 = logPack_stream3.flatMap(new PDMParser()).setParallelism(6);
//        DataStream<PDM.Log> log_stream4 = logPack_stream4.flatMap(new PDMParser()).setParallelism(6);
//        DataStream<PDM.Log> log_stream5 = logPack_stream5.flatMap(new PDMParser()).setParallelism(6);
//        DataStream<PDM.Log> log_stream6 = logPack_stream6.flatMap(new PDMParser()).setParallelism(6);
//        DataStream<PDM.Log> log_stream7 = logPack_stream7.flatMap(new PDMParser()).setParallelism(6);
//        DataStream<PDM.Log> log_stream8 = logPack_stream8.flatMap(new PDMParser()).setParallelism(6);
//        DataStream<PDM.Log> log_stream9 = logPack_stream9.flatMap(new PDMParser()).setParallelism(6);
//        DataStream<PDM.Log> log_stream10 = logPack_stream10.flatMap(new PDMParser()).setParallelism(6);


        event_stream = log_stream.map(PDMParser::initAssociatedEvent).setParallelism(6);
//        DataStream<AssociatedEvent> event_stream2 = log_stream2.map(PDMParser::initAssociatedEvent).setParallelism(6);
//        DataStream<AssociatedEvent> event_stream3 = log_stream3.map(PDMParser::initAssociatedEvent).setParallelism(6);
//        DataStream<AssociatedEvent> event_stream4 = log_stream4.map(PDMParser::initAssociatedEvent).setParallelism(6);
//        DataStream<AssociatedEvent> event_stream5 = log_stream5.map(PDMParser::initAssociatedEvent).setParallelism(6);
//        DataStream<AssociatedEvent> event_stream6 = log_stream6.map(PDMParser::initAssociatedEvent).setParallelism(6);
//        DataStream<AssociatedEvent> event_stream7 = log_stream7.map(PDMParser::initAssociatedEvent).setParallelism(6);
//        DataStream<AssociatedEvent> event_stream8 = log_stream8.map(PDMParser::initAssociatedEvent).setParallelism(6);
//        DataStream<AssociatedEvent> event_stream9 = log_stream9.map(PDMParser::initAssociatedEvent).setParallelism(6);
//        DataStream<AssociatedEvent> event_stream10 = log_stream10.map(PDMParser::initAssociatedEvent).setParallelism(6);

        event_stream.keyBy(associatedEvent -> associatedEvent.hostUUID)
                .process(new GraphAlignmentProcessFunction()).setParallelism(6);
//
//        event_stream2.keyBy(associatedEvent -> associatedEvent.hostUUID)
//                .process(new GraphAlignmentProcessFunction()).setParallelism(6);
//
//        event_stream3.keyBy(associatedEvent -> associatedEvent.hostUUID)
//                .process(new GraphAlignmentProcessFunction()).setParallelism(6);
//
//        event_stream4.keyBy(associatedEvent -> associatedEvent.hostUUID)
//                .process(new GraphAlignmentProcessFunction()).setParallelism(6);
//
//        event_stream5.keyBy(associatedEvent -> associatedEvent.hostUUID)
//                .process(new GraphAlignmentProcessFunction()).setParallelism(6);
//
//        event_stream6.keyBy(associatedEvent -> associatedEvent.hostUUID)
//                .process(new GraphAlignmentProcessFunction()).setParallelism(6);
//
//        event_stream7.keyBy(associatedEvent -> associatedEvent.hostUUID)
//                .process(new GraphAlignmentProcessFunction()).setParallelism(6);
//
//        event_stream8.keyBy(associatedEvent -> associatedEvent.hostUUID)
//                .process(new GraphAlignmentProcessFunction()).setParallelism(6);
//
//        event_stream9.keyBy(associatedEvent -> associatedEvent.hostUUID)
//                .process(new GraphAlignmentProcessFunction()).setParallelism(6);
//
//        event_stream10.keyBy(associatedEvent -> associatedEvent.hostUUID)
//                .process(new GraphAlignmentProcessFunction()).setParallelism(6);
        // Execute the Flink job
        env.execute("Online Flink");
    }

//    public void getCpu() {
//        OperatingSystemMXBean mem = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
//        System.out.println("CPU使用率:" + mem.getSystemCpuLoad());
//        System.out.println("进程占用CPU：" + mem.getProcessCpuLoad());
//    }

}
