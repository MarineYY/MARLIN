package libtagpropagation.graphalignment.alignmentstatus;

import com.google.common.collect.Iterators;
import libtagpropagation.graphalignment.techniqueknowledgegraph.SeedNode;
import libtagpropagation.graphalignment.techniqueknowledgegraph.TechniqueKnowledgeGraph;
import org.apache.flink.api.java.tuple.Tuple2;
import provenancegraph.AssociatedEvent;

import java.util.ArrayList;
import java.util.Arrays;

public class GraphAlignmentStatus {
    // 需要从 GraphAlignmentTag 中剥离出来以实现二次索引
    // 用于记录匹配结果，并计算匹配的分数，在不同 Tag 之间共用
    // 用 TechniqueKnowledgeGraph 初始化
    // 输入匹配上的结果，该结果来自于 TechniqueKnowledge 和 ALignmentSearchTree 的输出
    // 输出匹配的最新情况
    public String techniqueName;
    public Float alignmentScore = 0.0F;
    public boolean ALERT_FLAG;
    public static final Float ALIGNMENT_THRESHOLDS = 1.4F;

    private int nodeCount;
    private int edgeCount;

    private NodeAlignmentStatus[] nodeAlignmentStatusList;
    private EdgeAlignmentStatus[] edgeAlignmentStatusList;

    public GraphAlignmentStatus(Tuple2<SeedNode, TechniqueKnowledgeGraph> entry) {
        nodeCount = Iterators.size(entry.f1.tinkerGraph.getVertices().iterator());
        nodeAlignmentStatusList = new NodeAlignmentStatus[nodeCount];
        Arrays.fill(nodeAlignmentStatusList, null);
        nodeAlignmentStatusList[entry.f0.getId()] = new NodeAlignmentStatus(entry.f0.getType(), entry.f0.getAlignedString());

        edgeCount = Iterators.size(entry.f1.tinkerGraph.getEdges().iterator());
        edgeAlignmentStatusList = new EdgeAlignmentStatus[edgeCount];
        Arrays.fill(edgeAlignmentStatusList, null);

        this.techniqueName = entry.f1.techniqueName;
        this.ALERT_FLAG = false;
    }

    public GraphAlignmentStatus tryUpdateStatus(int nodeIndex, int edgeIndex, NodeAlignmentStatus newNodeAlignmentStatus, ArrayList<AssociatedEvent> cachedPath) {
        if (nodeIndex >= nodeCount || edgeIndex >= edgeCount) throw new RuntimeException("This node or edge seems not in the TKG.");

        // ToDo：考虑边的匹配状态
        if (edgeAlignmentStatusList[edgeIndex] == null){
            this.nodeAlignmentStatusList[nodeIndex] = newNodeAlignmentStatus;//Fixme: 节点未必为空
            this.edgeAlignmentStatusList[edgeIndex] = new EdgeAlignmentStatus(cachedPath, nodeIndex);
            this.alignmentScore += newNodeAlignmentStatus.getAlignmentScore() * (1.0f / cachedPath.size() + 1) / this.edgeCount;
            this.edgeAlignmentStatusList[edgeIndex].setAnomlyScore((1.0f / cachedPath.size() + 1) / this.edgeCount);
        }
        else{
//            Float newEdgeAlignmentScore = newNodeAlignmentStatus.getAlignmentScore() / cachedPath.size();
//            Float originalAlignmentScore = this.nodeAlignmentStatusList[nodeIndex].getAlignmentScore() / this.edgeAlignmentStatusList[edgeIndex].getPathLength();
            Float newEdgeAlignmentScore = newNodeAlignmentStatus.getAlignmentScore() * (1.0f / cachedPath.size() + 1) / this.edgeCount;
            Float originalAlignmentScore = this.nodeAlignmentStatusList[nodeIndex].getAlignmentScore() * this.edgeAlignmentStatusList[edgeIndex].getAnomlyScore();
            if (newEdgeAlignmentScore > originalAlignmentScore){
                this.edgeAlignmentStatusList[edgeIndex] = new EdgeAlignmentStatus(cachedPath, nodeIndex);
                this.nodeAlignmentStatusList[nodeIndex] = newNodeAlignmentStatus;
                this.alignmentScore = this.alignmentScore - originalAlignmentScore + newEdgeAlignmentScore;
            }
            else return null;
        }
        return this;
    }

    public NodeAlignmentStatus[] getNodeAlignmentStatusList() {
        return nodeAlignmentStatusList;
    }

    public EdgeAlignmentStatus[] getEdgeAlignmentStatusList() {
        return edgeAlignmentStatusList;
    }

    public boolean shouldTriggerAlert(){
        return alignmentScore >= ALIGNMENT_THRESHOLDS;
    }

    public boolean recurringAlert(){
        return this.ALERT_FLAG;
    }
    public String getAlignmentResult() {
        this.ALERT_FLAG = true;
        // 格式化的输出对齐的结果，作为告警信息
        StringBuilder alignmentResult = new StringBuilder();
        alignmentResult.append("Alert\nTKG: ").append(this.techniqueName).append("\n");
        alignmentResult.append("alignmentScore: ").append(this.alignmentScore).append("\n");
        int count = 0;
        for (NodeAlignmentStatus nodeAligned : nodeAlignmentStatusList) {
            if (nodeAligned == null) continue;
            else alignmentResult.append(count).append(" node: ").append(nodeAligned.toString()).append("\n");
            count ++;
        }
        count = 0;
        for (EdgeAlignmentStatus edgeAligned : edgeAlignmentStatusList) {
            if (edgeAligned == null) continue;
            else alignmentResult.append(count).append(" edge: ").append(edgeAligned.toString()).append("\n");
            count++;
        }

        return alignmentResult.toString();
    }

    public void print(){
        System.out.println("TKG: " + this.techniqueName);
        System.out.println("score: " + this.alignmentScore);
        String str = "null";
        for (int i = 0; i < nodeCount; i ++){
            if (nodeAlignmentStatusList[i] != null) str =  nodeAlignmentStatusList[i].toString();
            System.out.println("node " + i + ": " + str);
            str = "null";
        }

        for (int i = 0; i < edgeCount; i ++){
            if (edgeAlignmentStatusList[i] != null) str = edgeAlignmentStatusList[i].toString();
            System.out.println("edge " + i + ": " + str);
            str = "null";
        }
        System.out.println();
    }

    public GraphAlignmentStatus mergeAlignmentStatus(EdgeAlignmentStatus[] anotherEdgeAlignmentStatusList, NodeAlignmentStatus[] anotherNodeAlignmentStatusList){
//        for (int i = 0; i < anotherNodeAlignmentStatusList.length; i ++){
//            if (anotherNodeAlignmentStatusList[i] != null && nodeAlignmentStatusList[i] == null)
//                nodeAlignmentStatusList[i] = anotherNodeAlignmentStatusList[i];
//        }

        for (int i = 0; i < anotherEdgeAlignmentStatusList.length; i ++){
            if(anotherEdgeAlignmentStatusList[i] != null && edgeAlignmentStatusList[i] == null){
                edgeAlignmentStatusList[i] = anotherEdgeAlignmentStatusList[i];
                int nodeAlignmentStatusIndex = anotherEdgeAlignmentStatusList[i].getNodeAlignmentStatusIndex();
                this.nodeAlignmentStatusList[nodeAlignmentStatusIndex] = anotherNodeAlignmentStatusList[nodeAlignmentStatusIndex];
                this.alignmentScore += anotherEdgeAlignmentStatusList[i].getAnomlyScore();
            }
            else if (anotherEdgeAlignmentStatusList[i] != null && edgeAlignmentStatusList[i] != null){
                Float newEdgeAlignmentScore = anotherEdgeAlignmentStatusList[i].getAnomlyScore();
                Float originalAlignmentScore = this.edgeAlignmentStatusList[i].getAnomlyScore();
                if (newEdgeAlignmentScore > originalAlignmentScore){
                    this.edgeAlignmentStatusList[i] = anotherEdgeAlignmentStatusList[i];
                    int nodeAlignmentStatusIndex = anotherEdgeAlignmentStatusList[i].getNodeAlignmentStatusIndex();
                    this.nodeAlignmentStatusList[nodeAlignmentStatusIndex] = anotherNodeAlignmentStatusList[nodeAlignmentStatusIndex];
                    this.alignmentScore = this.alignmentScore - originalAlignmentScore + newEdgeAlignmentScore;
                }
            }
        }
        return this;
    }

}
