package libtagpropagation.graphalignment;

import libtagpropagation.graphalignment.techniqueknowledgegraph.SeedNode;
import libtagpropagation.graphalignment.techniqueknowledgegraph.TechniqueKnowledgeGraph;
import libtagpropagation.graphalignment.techniqueknowledgegraph.TechniqueKnowledgeGraphSeedSearching;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.common.protocol.types.Field;
import provenancegraph.*;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.*;

public class GraphAlignmentProcessFunction
        extends KeyedProcessFunction<UUID, AssociatedEvent, String>{

    private static final String knowledgeGraphPath = "TechniqueKnowledgeGraph/ManualCrafted";
    private transient ListState<TechniqueKnowledgeGraph> tkgList;
    private transient ValueState<TechniqueKnowledgeGraphSeedSearching> seedSearching; // ToDo：和直接存变量有什么区别？

    private transient ValueState<Boolean> isInitialized;
    private transient MapState<UUID, GraphAlignmentMultiTag> tagsCacheMap;
    public static int processEventCount = 0;
    public static int initTagCount = 0;
    public static int propagateTagCount = 0;
    public static int multiTagCount = 0;
    public static int tagCount = 0;
    public static int removeTagCount = 0;
    private Long currentTimeStamp;
    private String outPath;

    @Override
    public void open(Configuration parameter) {
//        StateTtlConfig ttlConfig = StateTtlConfig
//                .newBuilder(Time.seconds(1))
//                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) // 处理时间， 事件时间
//                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //
//                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                .cleanupFullSnapshot()
//                .build();

        ValueStateDescriptor<Boolean> isInitializedDescriptor =
                new ValueStateDescriptor<>("isInitialized", Boolean.class, false);
        this.isInitialized = getRuntimeContext().getState(isInitializedDescriptor);

        ListStateDescriptor<TechniqueKnowledgeGraph> tkgListDescriptor =
                new ListStateDescriptor<>("tkgListStatus", TechniqueKnowledgeGraph.class);
        this.tkgList = getRuntimeContext().getListState(tkgListDescriptor);

        ValueStateDescriptor<TechniqueKnowledgeGraphSeedSearching> seedSearchingDescriptor =
                new ValueStateDescriptor<>("seedSearching", TechniqueKnowledgeGraphSeedSearching.class, null);
        this.seedSearching = getRuntimeContext().getState(seedSearchingDescriptor);

        MapStateDescriptor<UUID, GraphAlignmentMultiTag> tagCacheStateDescriptor =
                new MapStateDescriptor<>("tagsCacheMap", UUID.class, GraphAlignmentMultiTag.class);
//        tagCacheStateDescriptor.enableTimeToLive(ttlConfig);
        tagsCacheMap = getRuntimeContext().getMapState(tagCacheStateDescriptor);
    }


    @Override
    public void processElement(AssociatedEvent associatedEvent,
                               KeyedProcessFunction<UUID, AssociatedEvent, String>.Context context,
                               Collector<String> collector) throws Exception {
        processEventCount++;
        tagCount = propagateTagCount + initTagCount;
        if (processEventCount % 100000 == 0){
            System.out.println("处理的事件数量：" + processEventCount + "\n多标签数量： " + multiTagCount + "\n标签数量： " + tagCount
                    + "\n初始化标签数量： " + initTagCount + "  传播标签数量： " + propagateTagCount + "  移除标签的数量:  " + removeTagCount
                    + "\n当前时间： " + associatedEvent.timeStamp
                    + "\n...\n"
            );
        }

        /*
        traverse all tags
         */
//        try {
//            Iterator<Map.Entry<UUID, GraphAlignmentMultiTag>> iteratorMultiTags = tagsCacheMap.entries().iterator();
//            while(iteratorMultiTags.hasNext()){
//                Map.Entry<UUID, GraphAlignmentMultiTag> entry = iteratorMultiTags.next();
//                Iterator<Map.Entry<String, GraphAlignmentTag>> iteratorTags = entry.getValue().getTagMap().entrySet().iterator();
//                while(iteratorTags.hasNext()){
//                    Map.Entry<String, GraphAlignmentTag> tagEntry = iteratorTags.next();
//                    if (tagEntry.getValue().getLastAccessTime() + (4.0 * 3600000000000L) < associatedEvent.timeStamp) {
//                        iteratorTags.remove();
//                    }
//                }
//            }
//        }catch (NullPointerException e)
//        {
//            e.printStackTrace();
//        }

        /*
        implement statistics of active tags
         */
//        if (!Objects.isNull(currentTimeStamp))
//        {
//            if (associatedEvent.timeStamp >= currentTimeStamp + 60000000000L){
//
//                currentTimeStamp = associatedEvent.timeStamp;
//
//                StringBuilder activeTagStatistic = new StringBuilder("activeTagStatistic:\n");
//                Integer counter = 0;
//                for (Map.Entry<UUID, GraphAlignmentMultiTag> entry : tagsCacheMap.entries()){
//                    counter += entry.getValue().getTagMap().size();
//                }
//                activeTagStatistic.append(counter).append("\n");
//
//                try {
//                    if (Objects.isNull(outPath)) {
//                        outPath = "D:\\Program File\\git_repository\\dataFiles\\outData\\" + System.currentTimeMillis() + ".txt";
//                    }
//                    File file = new File(outPath);
//                    BufferedWriter outWriter = new BufferedWriter(new FileWriter(file, true));
//                    outWriter.write(activeTagStatistic.toString());
//                    outWriter.flush();
//                    outWriter.close();
//                }catch (Exception e){
//                    e.printStackTrace();
//                }
//            }
//        }

        if (!this.isInitialized.value()){
            currentTimeStamp = associatedEvent.timeStamp;
            init(knowledgeGraphPath);
            this.isInitialized.update(true);
            System.out.println("\nOnline Alignment start ......\n");
        }
        try {
            tryInitGraphAlignmentTag(associatedEvent);
            propagateGraphAlignmentTag(associatedEvent);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init(String knowledgeGraphPath) {
        try {
            loadTechniqueKnowledgeGraphList(knowledgeGraphPath);
            int count = 0;
            String out = "";
            for (TechniqueKnowledgeGraph tkg : this.tkgList.get()){
                count ++;
                out += tkg.techniqueName + "\n";
            }
            System.out.println("TKG 总数是 ：" + count + "\n" + out);
            this.seedSearching.update(new TechniqueKnowledgeGraphSeedSearching(this.tkgList.get()));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void loadTechniqueKnowledgeGraphList(String kgFilesPath) throws Exception {
        File file = new File(kgFilesPath);
        File[] kgFileList = file.listFiles();
        assert kgFileList != null;
        for (File kgFile : kgFileList) {
            String kgFileName = kgFile.toString();
            TechniqueKnowledgeGraph tkg = new TechniqueKnowledgeGraph(kgFileName);
            this.tkgList.add(tkg);
        }
    }

    private GraphAlignmentMultiTag tryInitGraphAlignmentTag(AssociatedEvent associatedEvent) throws Exception {
        Set<Tuple2<SeedNode, TechniqueKnowledgeGraph>> initTkgList = new HashSet<>();

        initTkgList.addAll(this.seedSearching.value().search(associatedEvent.sourceNode));
        // 记录到source上传播到sink上面
        initTkgList.addAll(this.seedSearching.value().search(associatedEvent));

        if (initTkgList.isEmpty()) return null;
        else {
            GraphAlignmentMultiTag multiTag = new GraphAlignmentMultiTag(initTkgList, associatedEvent.sourceNode.getNodeUUID(), associatedEvent.timeStamp);
            if (this.tagsCacheMap.contains(associatedEvent.sourceNode.getNodeUUID())) {
                this.tagsCacheMap.get(associatedEvent.sourceNode.getNodeUUID()).mergeMultiTag(multiTag);
            }
            else{
                this.tagsCacheMap.put(associatedEvent.sourceNode.getNodeUUID(), multiTag);
                multiTagCount ++;
            }
            return multiTag;
        }
    }

    private GraphAlignmentMultiTag propagateGraphAlignmentTag(AssociatedEvent associatedEvent) throws Exception {
        GraphAlignmentMultiTag srcMultiTag = tagsCacheMap.get(associatedEvent.sourceNode.getNodeUUID());
        if (srcMultiTag != null) {
            if (srcMultiTag.getTagMap().size() == 0) {
                this.tagsCacheMap.remove(associatedEvent.sourceNode.getNodeUUID());
            }
            GraphAlignmentMultiTag sinkMultiTag = tagsCacheMap.get(associatedEvent.sinkNode.getNodeUUID());
            GraphAlignmentMultiTag newTags = srcMultiTag.propagate(associatedEvent);

            // merge tag
            if (sinkMultiTag == null) {
                if (newTags.getTagMap().size() > 0){
                    this.tagsCacheMap.put(associatedEvent.sinkNode.getNodeUUID(), newTags);
                    multiTagCount ++;
                }
            } else {
                sinkMultiTag.mergeMultiTag(newTags);
                newTags.mergeMultiTag(sinkMultiTag);
            }
        }
        return tagsCacheMap.get(associatedEvent.sinkNode.getNodeUUID());
    }

    @Override
    public String toString() {
        return "GraphAlignmentProcessFunction{" +
                "tkgList=" + tkgList +
                ", seedSearching=" + seedSearching +
                ", isInitialized=" + isInitialized +
                ", tagsCacheMap=" + tagsCacheMap +
                '}';
    }
}


