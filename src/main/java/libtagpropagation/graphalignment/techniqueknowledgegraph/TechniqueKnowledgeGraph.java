package libtagpropagation.graphalignment.techniqueknowledgegraph;

//import com.sun.javaws.IconUtil;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;
import org.checkerframework.checker.units.qual.A;
import provenancegraph.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * Only consider TKG matching, not TKG generation.**/
/*
class TKGNode {
    private int nodeId;
    private UDMProto.LogType nodeType; // Process, File, Network for Linux
    private String tkgRegexRepresentation;

    TKGNode(UDMProto.LogType nodeType, String tkgRegexRepresentation) {
        this.nodeType = nodeType;
        this.tkgRegexRepresentation = tkgRegexRepresentation;
    }

    public boolean nodeMatching (String nodeRepresentation) {
        boolean isMatch = Pattern.matches(this.tkgRegexRepresentation, nodeRepresentation);
        return isMatch;
    }

    public float fuzzyNodeMatching(String nodeRepresentation){
        // ToDo:
        return 0.0F;
    }
}
*/

/**
 * The TechniqueKnowledge and Matching Status */
public class TechniqueKnowledgeGraph {
    // https://github.com/tinkerpop/blueprints/wiki/GraphML-Reader-and-Writer-Library
    public TinkerGraph tinkerGraph;

    public String techniqueName;
    public Float Alert_Threshold;

    private static final HashMap<String, String> KEY_PROPERTIES_MAP = new HashMap <String, String> (){{
        put("Network", "url_ip");
        put("File", "file_path");
        put("Process", "process_name");
    }};

    public TechniqueKnowledgeGraph(String gmlFilePath) throws IOException {
        this.loadFromGraphMLFile(gmlFilePath);
        String fileNameWithExtension = new File(gmlFilePath.trim()).getName();
        this.techniqueName = fileNameWithExtension.substring(0, fileNameWithExtension.lastIndexOf("."));
        this.Alert_Threshold = this.tinkerGraph.getVertex("n0").getProperty("Alert_threshold");
    }

    private void loadFromGraphMLFile(String gmlFilePath) throws IOException {
        /**
         * @Description Init TKG with GraphML file */
        if (gmlFilePath == null) {
            throw new FileNotFoundException();
        }

        this.tinkerGraph = new TinkerGraph();
        GraphMLReader reader = new GraphMLReader(this.tinkerGraph);

        InputStream is = new BufferedInputStream(Files.newInputStream(Paths.get(gmlFilePath)));
        reader.inputGraph(is);
    }

    public String printBasicInfo() {
        return this.tinkerGraph.toString();
    }

    public ArrayList<Object> getSeedObjects() {
        ArrayList<Object> seedObjects = new ArrayList<>();

        for (Vertex v: this.tinkerGraph.getVertices()) {
            try {
                if (v.getProperty("is_seed_node")) {
                    seedObjects.add(v);
                }
            }
            catch (NullPointerException e1) {
                e1.printStackTrace();
            }
        }

        for (Edge e: this.tinkerGraph.getEdges()) {
            try {
                if (e.getProperty("is_seed_edge")) {
                    seedObjects.add(e);
                }
            }
            catch (NullPointerException e2) {
                e2.printStackTrace();
            }
        }

        return seedObjects;
    }

    public ArrayList<Vertex> getVertexList() {
        ArrayList<Vertex> vertex_list = new ArrayList<>();
        for (Vertex v: this.tinkerGraph.getVertices()) {
            vertex_list.add(v);
        }
        return vertex_list;
    }

    public ArrayList<Edge> getEdgeList() {
        ArrayList<Edge> edge_list = new ArrayList<>();
        for (Edge e: this.tinkerGraph.getEdges()) {
            edge_list.add(e);
        }
        return edge_list;
    }

    public static String getKeyPropertiesFromType(String type) {
        return KEY_PROPERTIES_MAP.getOrDefault(type, null);
    }

    public static boolean nodeMatch(Vertex kgNode, Vertex pgNode) {
        String kgNodeType = kgNode.getProperty("type");
        String pgNodeType = pgNode.getProperty("type");
        if (!kgNodeType.equals(pgNodeType)) {
            return false;
        }
        String kpKGNode = kgNode.getProperty(getKeyPropertiesFromType(kgNode.getProperty("type")));
        String kpPGNode = pgNode.getProperty(getKeyPropertiesFromType(pgNode.getProperty("type")));
        return Pattern.matches(kpKGNode, kpPGNode);
    }

    public static boolean isVertexAligned(Vertex seedNode, BasicNode n, NodeProperties np) {
        TinkerGraph graph = new TinkerGraph();
        Vertex temp_node = graph.addVertex("1");
        switch (n.getNodeType()) {
            case "File":
                temp_node.setProperty("type", "File");
                temp_node.setProperty("file_path", ((FileNodeProperties) np).getFilePath());
                break;
            case "Process":
                temp_node.setProperty("type", "Process");
                temp_node.setProperty("process_name", ((ProcessNodeProperties) np).getProcessName());
                break;
            case "Network":
                temp_node.setProperty("type", "Network");
                temp_node.setProperty("url_ip", ((NetworkNodeProperties) np).getRemoteIp());
                break;
            default:
                break;
        }
        return nodeMatch(seedNode, temp_node);
    }

    public static boolean isEdgeAligned(Edge seedEdge, AssociatedEvent e) {
        String event = e.getRelationship();
        if (Pattern.matches(seedEdge.getProperty("event_type"), event)) {
            Vertex src = seedEdge.getVertex(Direction.OUT);
            Vertex dest = seedEdge.getVertex(Direction.IN);
            if (isVertexAligned(src, e.sourceNode, e.sourceNodeProperties)){
                return isVertexAligned(dest, e.sinkNode, e.sinkNodeProperties);
            }
        }
        return false;
    }

    public static float fuzzyNodeMatch(Vertex source, Vertex target){
        // ToDo:
        return 0.0F;
    }
}
