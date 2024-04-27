package libtagpropagation.graphalignment.techniqueknowledgegraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import libtagpropagation.graphalignment.alignmentstatus.NodeAlignmentStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import provenancegraph.AssociatedEvent;
import java.util.ArrayList;
import java.util.HashMap;


public class AlignmentSearchGraph {
    // 用于加速 TechniqueKnowledgeGraph 的搜索速度
    // 输入：查询的起点，即 Tag 中缓存的 lastAlignedNode，以及新到的节点
    // 输出：匹配的分数（0表示没有匹配上），匹配到节点的新位置
    private ArrayList<ArrayList<SeedEdge>> edgeSearch;
    private ArrayList<SeedEdge> edgeList;
    private HashMap<String, Boolean> nodeStatus;

    public AlignmentSearchGraph(String alignedString, TechniqueKnowledgeGraph tkg) {
        this.nodeStatus = new HashMap<>();
        this.edgeList = new ArrayList<>();

        // set size of dynamic graphSearch
        this.edgeSearch = new ArrayList<>(tkg.getVertexList().size());
        for (int i = 0; i < tkg.getVertexList().size(); i ++ ){
            this.edgeSearch.add(new ArrayList<>());
        }

        // init nodeSearch via edgeList
        for (Edge edge : tkg.getEdgeList()){
            SeedEdge seedEdge = new SeedEdge(edge);
            this.nodeStatus.put(seedEdge.getSourceNode().getMatchedString(), false);
            this.edgeList.add(seedEdge);
            this.edgeSearch.get(seedEdge.getSourceNode().getId()).add(seedEdge);
        }

        this.nodeStatus.put(alignedString, true);
    }

    public Tuple3<NodeAlignmentStatus, NodeAlignmentStatus, Integer> alignmentSearch(int lastAlignedNodeIndex, AssociatedEvent currentEdege) {

        // 处理合并时缺少lastAlignedNode的情况
        if (lastAlignedNodeIndex < 0){
            for (SeedEdge seedEdge : this.edgeList){
                if (seedEdge.isNextEdgeAligned(currentEdege)){
                    if (this.nodeStatus.get(seedEdge.getSourceNode().getMatchedString())){
                        NodeAlignmentStatus nodeSinkAlignmentStatus = new NodeAlignmentStatus(seedEdge.getSinkNode().getType(), seedEdge.getSinkNode().getAlignedString(), seedEdge.getSinkNode().getId());
                        NodeAlignmentStatus nodeSourceAlignmentStatus = new NodeAlignmentStatus(seedEdge.getSourceNode().getType(), seedEdge.getSourceNode().getAlignedString(), seedEdge.getSourceNode().getId());
                        this.nodeStatus.put(seedEdge.getSinkNode().getAlignedString(), true);
                        return Tuple3.of(nodeSourceAlignmentStatus, nodeSinkAlignmentStatus, seedEdge.getId());
                    }
                }
            }
        }else{
            ArrayList<SeedEdge> edges = this.edgeSearch.get(lastAlignedNodeIndex);
            for (SeedEdge seedEdge : edges){
                if (seedEdge.isNextEdgeAligned(currentEdege)){//revise the matching approach from one edge mapping one edge to one node mapping node.
                    NodeAlignmentStatus nodeSinkAlignmentStatus = new NodeAlignmentStatus(seedEdge.getSinkNode().getType(), seedEdge.getSinkNode().getAlignedString(), seedEdge.getSinkNode().getId());
                    NodeAlignmentStatus nodeSourceAlignmentStatus = new NodeAlignmentStatus(seedEdge.getSourceNode().getType(), seedEdge.getSourceNode().getAlignedString(), seedEdge.getSourceNode().getId());
                    this.nodeStatus.put(seedEdge.getSinkNode().getAlignedString(), true);
                    return Tuple3.of(nodeSourceAlignmentStatus, nodeSinkAlignmentStatus, seedEdge.getId());
                }
            }
        }

        return null;
    }

}
