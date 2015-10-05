package net.stemmaweb.services;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import net.stemmaweb.printer.GraphViz;
import net.stemmaweb.rest.ERelations;

import net.stemmaweb.rest.Nodes;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Uniqueness;

/**
 * This class provides methods for exporting Dot File from Neo4J
 * 
 * @author PSE FS 2015 Team2
 */
public class Neo4JToDotParser
{
    private GraphDatabaseService db;

    private OutputStream out = null;

    public Neo4JToDotParser(GraphDatabaseService db){
        this.db = db;
    }

    public Response parseNeo4J(String tradId)
    {
        String filename = "upload/output.dot";

        Node startNode = DatabaseService.getStartNode(tradId, db);

        try (Transaction tx = db.beginTx()) {
            if(startNode==null) {
                return Response.status(Status.NOT_FOUND).build();
            }

            File file = new File(filename);

            file.createNewFile();
            out = new FileOutputStream(file);

            write("digraph { ");

            long edgeId = 0;
            String subgraph = "";
            for (Node node : db.traversalDescription().breadthFirst()
                    .relationships(ERelations.SEQUENCE,Direction.OUTGOING)
                    .uniqueness(Uniqueness.NODE_GLOBAL)
                    .traverse(startNode)
                    .nodes()) {

                write("n" + node.getId() + " [label=\"" + node.getProperty("text").toString()
                        + "\"];");

                for(Relationship rel : node.getRelationships(Direction.OUTGOING,ERelations.SEQUENCE)) {
                    if(rel != null && rel.hasProperty("witnesses")) {
                        String[] witnesses = (String[]) rel.getProperty("witnesses");
                        String lex_str = "";
                        Iterator<String> it = Arrays.asList(witnesses).iterator();
                        while(it.hasNext()) {
                            lex_str += "" + it.next() + "";
                            if(it.hasNext()) {
                                lex_str += ",";
                            }
                        }
                        write("n" + rel.getStartNode().getId() + "->" + "n" +
                                rel.getEndNode().getId() + " [label=\""+ lex_str +"\", id=\"e"+
                                edgeId++ +"\"];");
                    }
                }
                for(Relationship rel : node.getRelationships(Direction.OUTGOING,
                        ERelations.RELATED)) {
                    subgraph += "n" + rel.getStartNode().getId() + "->" + "n" +
                            rel.getEndNode().getId() + " [style=dotted, label=\""+
                            rel.getProperty("type").toString() +"\", id=\"e"+ edgeId++ +"\"];";
                }
            }

            write("subgraph { edge [dir=none]");
            write(subgraph);
            write(" } }");

            out.flush();
            out.close();

            tx.success();
        } catch (IOException e) {
            e.printStackTrace();
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Could not write file for export")
                    .build();
        }

        // writePNGFromDotFile(filename,"upload/file");
        // writeSVGFromDotFile(filename,"upload/file");

        return Response.ok().build();
    }

    /**
     * Parses a Stemma of a tradition in a JSON string in DOT format
     * don't throw error far enough
     *
     * @param tradId
     * @param stemmaTitle
     * @return
     */
    public Response parseNeo4JStemma(String tradId, String stemmaTitle)
    {
        String output;
        String outputNodes="";
        String outputRelationships="";

        try (Transaction tx = db.beginTx()) {
            Node traditionNode = db.findNode(Nodes.TRADITION, "id", tradId);
            Node startNodeStemma = null;
            for (Node stemma : DatabaseService.getRelated(traditionNode, ERelations.HAS_STEMMA)) {
                if (stemma.getProperty("name").equals(stemmaTitle)) {
                    startNodeStemma = stemma;
                    break;
                }
            }
            if(startNodeStemma == null) {
                return Response.status(Status.NOT_FOUND).build();
            }

            String stemmaType = (Boolean) startNodeStemma.getProperty("directed") ? "digraph" : "graph";
            String edgeGlyph = (Boolean) startNodeStemma.getProperty("directed") ? "->" : "--";
            outputNodes += String.format("%s \"%s\" {\n", stemmaType, stemmaTitle);

            // Output all the nodes associated with this stemma.
            for (Node witness : DatabaseService.getRelated(startNodeStemma, ERelations.HAS_WITNESS)) {
                Boolean witnessExists = !(witness.hasProperty("hypothetical")
                        && (Boolean) witness.getProperty("hypothetical"));
                String witnessClass = witnessExists ? "extant" : "hypothetical";
                outputNodes += String.format("\t%s [class=%s];\n", witness.getProperty("sigil"), witnessClass);
            }

            // Now output all the edges associated with this stemma, starting with the
            // archetype if we have one.
            ArrayList<Node> foundRoots = DatabaseService.getRelated(startNodeStemma, ERelations.HAS_ARCHETYPE);
            if (foundRoots.isEmpty()) {
                // No archetype; just output the list of edges in any order.
                Result txEdges = db.execute("MATCH (a:WITNESS)-[:TRANSMITTED {hypothesis:'" +
                        stemmaTitle + "'}]->(b:WITNESS) RETURN a.sigil, b.sigil");
                while (txEdges.hasNext()) {
                    Map<String, Object> vector = txEdges.next();
                    String source = vector.get("a.sigil").toString();
                    String target = vector.get("b.sigil").toString();
                    outputRelationships += String.format("\t%s %s %s;\n", source, edgeGlyph, target);
                }
            } else {
                // We have an archetype; start there and traverse the graph.
                Node stemmaRoot = foundRoots.get(0);  // There should be only one.
                for (Vector v : traverseStemma(startNodeStemma, stemmaRoot)) {
                    outputRelationships += String.format("\t%s %s %s;\n", v.source(), edgeGlyph, v.target());
                }
            }
            output = outputNodes + outputRelationships + "}\n";

            tx.success();
        }

        // writePNGFromDot(output,"upload/file");
        writeSVGFromDot(output, "upload/file");

        return Response.ok(output).build();
    }

    public String getAllStemmataAsDot(String tradId) {
        String dot = "";

        try(Transaction tx = db.beginTx()) {
            //ExecutionEngine engine = new ExecutionEngine(db);
            // find all Stemmata associated with this tradition
            Result result = db.execute("match (t:TRADITION {id:'"+ tradId +
                    "'})-[:HAS_STEMMA]->(s:STEMMA) return s");

            Iterator<Node> stemmata = result.columnAs("s");
            while(stemmata.hasNext()) {
                String stemma = stemmata.next().getProperty("name").toString();
                Response resp = parseNeo4JStemma(tradId, stemma);

                dot = dot + resp.getEntity();
            }
            tx.success();
        }

        return dot;
    }

    private Set<Vector> traverseStemma(Node stemma, Node archetype) {
        String stemmaName = (String) stemma.getProperty("name");
        Set<Vector> allPaths = new HashSet<>();

        // We need to traverse only those paths that belong to this stemma.
        PathExpander e = new PathExpander() {
            @Override
            public Iterable<Relationship> expand(Path path, BranchState branchState) {
                ArrayList<Relationship> goodPaths = new ArrayList<>();
                Iterator<Relationship> stemmaLinks = path.endNode()
                        .getRelationships(ERelations.TRANSMITTED, Direction.BOTH).iterator();
                while(stemmaLinks.hasNext()) {
                    Relationship link = stemmaLinks.next();
                    if (link.getProperty("hypothesis").equals(stemmaName)) {
                        goodPaths.add(link);
                    }
                }
                return goodPaths;
            }

            @Override
            public PathExpander reverse() {
                return null;
            }
        };
        for (Path nodePath: db.traversalDescription().breadthFirst()
                .expand(e)
                .uniqueness(Uniqueness.NODE_PATH)
                .traverse(archetype)) {
            Iterator<Node> orderedNodes = nodePath.nodes().iterator();
            Node sourceNode = orderedNodes.next();
            while (orderedNodes.hasNext()) {
                Node targetNode = orderedNodes.next();
                String source = (String) sourceNode.getProperty("sigil");
                String target = (String) targetNode.getProperty("sigil");
                allPaths.add(new Vector(source, target));
                sourceNode = targetNode;
            }
        }
        return allPaths;
    }

    private final class Vector {
        String x;
        String y;

        public Vector(String from, String to) {
            x = from;
            y = to;
        }

        public String source() {
            return x;
        }
        public String target() {
            return y;
        }
    }

    private void write(String str) throws IOException
    {
        out.write(str.getBytes());
    }


    private void writePNGFromDot(String dot, String outFile)
    {
        GraphViz gv = new GraphViz();
        gv.add(dot);

        String type = "png";

        File out = new File(outFile + "." + type);   // Linux
//	      File out = new File("c:/eclipse.ws/graphviz-java-api/out." + type);    // Windows
        gv.writeGraphToFile( gv.getGraph( gv.getDotSource(), type ), out );
    }

    private void writeSVGFromDot(String dot, String outFile)
    {
        GraphViz gv = new GraphViz();
        gv.add(dot);

        String type = "svg";

        File out = new File(outFile + "." + type);   // Linux
//	      File out = new File("c:/eclipse.ws/graphviz-java-api/out." + type);    // Windows
        gv.writeGraphToFile( gv.getGraph( gv.getDotSource(), type ), out );
    }

}