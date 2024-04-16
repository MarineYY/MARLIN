package libtagpropagation.graphalignment;

import libtagpropagation.graphalignment.techniqueknowledgegraph.SeedNode;
import libtagpropagation.graphalignment.techniqueknowledgegraph.TechniqueKnowledgeGraph;
import org.apache.flink.api.java.tuple.Tuple2;
import provenancegraph.AssociatedEvent;

import java.util.*;

import static libtagpropagation.graphalignment.GraphAlignmentProcessFunction.removeTagCount;

public class GraphAlignmentMultiTag {
    private Map<String, GraphAlignmentTag> tagMap;

    public GraphAlignmentMultiTag(Set<Tuple2<SeedNode, TechniqueKnowledgeGraph>> tkgList, UUID sourceUUID, Long eventTime) {
        tagMap = new HashMap<>();

        for (Tuple2<SeedNode, TechniqueKnowledgeGraph> entry : tkgList){
            GraphAlignmentTag tag = new GraphAlignmentTag(entry.f0, entry.f1, sourceUUID);
            tag.setLastAccessTime(eventTime);
            this.tagMap.put(entry.f1.techniqueName, tag);
        }
    }

    public GraphAlignmentMultiTag(GraphAlignmentMultiTag originalTag) {
        this.tagMap = new HashMap<>();
    }

    public GraphAlignmentMultiTag mergeMultiTag(GraphAlignmentMultiTag newMultiTag) {
        Map<String, GraphAlignmentTag> newMultiTagMap = newMultiTag.getTagMap();
        for (Map.Entry entry : newMultiTagMap.entrySet()){
            String key = (String) entry.getKey();
            if (this.tagMap.containsKey(key) && !this.tagMap.get(key).recurringAlert()) {
                GraphAlignmentTag mergedTag = this.tagMap.get(key).mergeTag((GraphAlignmentTag) entry.getValue());
                if (mergedTag == null) {
                    this.tagMap.remove(key);
                    removeTagCount += 2;
                }
                else {
                    this.tagMap.put(key, mergedTag);
                }
            }
            else
                this.tagMap.put((String) entry.getKey(), (GraphAlignmentTag) entry.getValue());
        }
        return this;
    }

    public GraphAlignmentMultiTag propagate(AssociatedEvent associatedEvent) {
        GraphAlignmentMultiTag newMultiTag = new GraphAlignmentMultiTag(this);
        Iterator<Map.Entry<String, GraphAlignmentTag>> iterator = this.tagMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, GraphAlignmentTag> entry = iterator.next();
            if (!(entry.getValue()).recurringAlert()) {
                String techniqueName = entry.getKey();
                GraphAlignmentTag newTag = entry.getValue().propagate(associatedEvent);
                if (newTag != null) {
                    newMultiTag.tagMap.put(techniqueName, newTag);
                } else {
                    iterator.remove();
                    removeTagCount += 2;
                }
            }
            else {
                iterator.remove();
                removeTagCount ++;
            }
        }

        return newMultiTag;
    }

    public Map<String, GraphAlignmentTag> getTagMap() {
        return tagMap;
    }
}
