package libtagpropagation.graphalignment.techniqueknowledgegraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import provenancegraph.AssociatedEvent;

import java.util.regex.Pattern;

public class SeedEdge {
    private int id;
    private String event_type;
    private SeedNode sourceNode;
    private SeedNode sinkNode;

    public SeedEdge(Edge seedEdge) {
        this.id = seedEdge.getProperty("sequence_num");
        this.event_type = seedEdge.getProperty("event_type");
        Vertex src = seedEdge.getVertex(Direction.OUT);
        Vertex sink = seedEdge.getVertex(Direction.IN);
        this.sourceNode = new SeedNode(src);
        this.sinkNode = new SeedNode(sink);
    }

    public boolean isEdgeAligned(AssociatedEvent e) {
        if (Pattern.matches(this.event_type , e.getRelationship())) {
            if (sourceNode.isNodeAligned(e.sourceNode, e.sourceNodeProperties)){
                return sinkNode.isNodeAligned(e.sinkNode, e.sinkNodeProperties);
            }
        }
        return false;
    }

    public boolean isNextEdgeAligned(AssociatedEvent e) {
//        if (Pattern.matches(this.event_type , e.getRelationship())) {
//            return sinkNode.isNodeAligned(e.sinkNode, e.sinkNodeProperties);
//        }
//        return false;
        return sinkNode.isNodeAligned(e.sinkNode, e.sinkNodeProperties);
    }

    public int getId(){
        return this.id;
    }

    @Override
    public String toString() {
        return "[" + sourceNode.toString() + "]->"+ event_type +"->[" + sinkNode.toString() + "]";
    }

    public SeedNode getSourceNode() {
        return sourceNode;
    }

    public SeedNode getSinkNode() {
        return sinkNode;
    }
}
