package net.stemmaweb.parser;

import net.stemmaweb.model.RelationTypeModel;
import net.stemmaweb.rest.ERelations;
import net.stemmaweb.rest.Nodes;
import net.stemmaweb.services.DatabaseService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.HashSet;

/**
 * Utility functions for the parsers
 * Created by tla on 14/02/2017.
 */
public class Util {

    // Start and end node creation
    static Node createStartNode(Node parentNode) {
        GraphDatabaseService db = parentNode.getGraphDatabase();
        Node startNode = db.createNode(Nodes.READING);
        startNode.setProperty("is_start", true);
        startNode.setProperty("section_id", parentNode.getId());
        startNode.setProperty("rank", 0L);
        startNode.setProperty("text", "#START#");
        parentNode.createRelationshipTo(startNode, ERelations.COLLATION);
        return startNode;
    }

    // Start and end node creation
    static Node createEndNode(Node parentNode) {
        GraphDatabaseService db = parentNode.getGraphDatabase();
        Node endNode = db.createNode(Nodes.READING);
        endNode.setProperty("is_end", true);
        endNode.setProperty("section_id", parentNode.getId());
        endNode.setProperty("text", "#END#");
        parentNode.createRelationshipTo(endNode, ERelations.HAS_END);
        return endNode;
    }

    // Witness node creation
    static Node createWitness(Node traditionNode, String sigil, Boolean hypothetical) {
        GraphDatabaseService db = traditionNode.getGraphDatabase();
        Node witnessNode = db.createNode(Nodes.WITNESS);
        witnessNode.setProperty("sigil", sigil);
        witnessNode.setProperty("hypothetical", hypothetical);
        witnessNode.setProperty("quotesigil", !isDotId(sigil));
        return witnessNode;
    }

    static Node createExtant(Node traditionNode, String sigil) {
        Node witnessNode = createWitness(traditionNode, sigil, false);
        traditionNode.createRelationshipTo(witnessNode, ERelations.HAS_WITNESS);
        return witnessNode;
    }

    private static Boolean isDotId (String nodeid) {
        return nodeid.matches("^[A-Za-z][A-Za-z0-9_.]*$")
                || nodeid.matches("^-?(\\.\\d+|\\d+\\.\\d+)$");
    }

    // XML parsing utilities
    static Document openFileStream(InputStream filestream) {
        try {
            return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(filestream);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Helper to get any existing SEQUENCE link between two readings.
    // NOTE: For use inside a transaction
    static Relationship getSequenceIfExists (Node source, Node target) {
        Relationship found = null;
        Iterable<Relationship> allseq = source.getRelationships(Direction.OUTGOING, ERelations.SEQUENCE);
        for (Relationship r : allseq) {
            if (r.getEndNode().equals(target)) {
                found = r;
                break;
            }
        }
        return found;
    }

    // Helper to set colocation flags on all colocated RELATED links.
    // NOTE: For use inside a transaction
    static void setColocationFlags (Node traditionNode) {
        HashSet<String> colocatedTypes = new HashSet<>();
        for (Relationship r : traditionNode.getRelationships(ERelations.HAS_RELATION_TYPE, Direction.OUTGOING)) {
            RelationTypeModel relType = new RelationTypeModel(r.getEndNode());
            if (relType.getIs_colocation()) colocatedTypes.add(relType.getName());
        }

        // Traverse the tradition looking for these types
        for (Relationship rel : DatabaseService.returnTraditionRelations(traditionNode).relationships()) {
            if (colocatedTypes.contains(rel.getProperty("type").toString()))
                rel.setProperty("colocation", true);
            else if (rel.hasProperty("colocation"))
                rel.removeProperty("colocation");
        }
    }

}
