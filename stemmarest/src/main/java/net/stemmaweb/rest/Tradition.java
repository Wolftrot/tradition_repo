package net.stemmaweb.rest;

import java.io.*;
import java.util.*;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.stream.XMLStreamException;

import net.stemmaweb.model.ReadingModel;
import net.stemmaweb.model.RelationshipModel;
import net.stemmaweb.model.TraditionModel;
import net.stemmaweb.model.WitnessModel;
import net.stemmaweb.services.*;
import net.stemmaweb.services.DatabaseService;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Uniqueness;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;


/**
 * Comprises all the api calls related to a tradition.
 * Can be called using http://BASE_URL/tradition
 * @author PSE FS 2015 Team2
 */

@Path("/tradition")
public class Tradition {
    private GraphDatabaseServiceProvider dbServiceProvider = new GraphDatabaseServiceProvider();
    private GraphDatabaseService db = dbServiceProvider.getDatabase();

    /**
     * Delegated API calls
     */

    @Path("/{tradId}/witness/{sigil}")
    public Witness getWitness(@PathParam("tradId") String tradId, @PathParam("sigil") String sigil) {
        return new Witness(tradId, sigil);
    }
    @Path("/{tradId}/stemma/{name}")
    public Stemma getStemma(@PathParam("tradId") String tradId, @PathParam("name") String name) {
        return new Stemma(tradId, name);
    }
    @Path("/{tradId}/relation")
    public Relation getRelation(@PathParam("tradId") String tradId) {
        return new Relation(tradId);
    }

    /**
     * Resource creation calls
     */
    @PUT  // a new stemma
    @Path("{tradId}/stemma")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response newStemma(@PathParam("tradId") String tradId, String dot) {
        DotToNeo4JParser parser = new DotToNeo4JParser(db);
        return parser.importStemmaFromDot(dot, tradId, false);
    }
    @POST  // a replacement stemma TODO test
    @Path("{tradId}/stemma")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response replaceStemma(@PathParam("tradId") String tradId, String dot) {
        DotToNeo4JParser parser = new DotToNeo4JParser(db);
        return parser.importStemmaFromDot(dot, tradId, true);
    }

    /*****************************
     * Collection retrieval calls
     */

    /**
     * Gets a list of all the witnesses of a tradition with the given id.
     *
     * @param tradId ID of the tradition to look up
     * @return Http Response 200 and a list of witness models in JSON on success
     *         or an ERROR in JSON format
     */
    @GET
    @Path("{tradId}/witnesses")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllWitnesses(@PathParam("tradId") String tradId) {
        Node traditionNode = getTraditionNode(tradId, db);
        if (traditionNode == null)
            return Response.status(Status.NOT_FOUND).entity("tradition not found").build();

        ArrayList<WitnessModel> witnessList = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            DatabaseService.getRelated(traditionNode, ERelations.HAS_WITNESS).forEach(r -> witnessList.add(new WitnessModel(r)));
            tx.success();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
        return Response.ok(witnessList).build();
    }

    /**
     * Gets a list of all Stemmata available, as dot format
     *
     * @param tradId - the tradition whose stemmata to retrieve
     * @return Http Response ok and a list of DOT JSON strings on success or an
     *         ERROR in JSON format
     */
    @GET
    @Path("{tradId}/stemmata")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllStemmata(@PathParam("tradId") String tradId) {
        Node traditionNode = DatabaseService.getTraditionNode(tradId, db);
        if (traditionNode == null)
            return Response.status(Status.NOT_FOUND).entity("No such tradition found").build();

        // find all stemmata associated with this tradition
        ArrayList<String> stemmata = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            DatabaseService.getRelated(traditionNode, ERelations.HAS_STEMMA)
                    .forEach(x -> stemmata.add(x.getProperty("name").toString()));
            tx.success();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
        Neo4JToDotParser parser = new Neo4JToDotParser(db);
        ArrayList<String> stemmataList = new ArrayList<>();
        stemmata.forEach( stemma -> {
                        Response localResp = parser.parseNeo4JStemma(tradId, stemma);
                        stemmataList.add((String) localResp.getEntity());
                    });

        return Response.ok(stemmataList).build();
    }

    /**
     * Gets a list of all relationships of a tradition with the given id.
     *
     * @param tradId ID of the tradition to look up
     * @return Http Response 200 and a list of relationship model in JSON
     */
    @GET
    @Path("{tradId}/relationships")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllRelationships(@PathParam("tradId") String tradId) {
        ArrayList<RelationshipModel> relList = new ArrayList<>();

        Node startNode = DatabaseService.getStartNode(tradId, db);
        try (Transaction tx = db.beginTx()) {
            db.traversalDescription().depthFirst()
                    .relationships(ERelations.SEQUENCE, Direction.OUTGOING)
                    .uniqueness(Uniqueness.NODE_GLOBAL)
                    .traverse(startNode).nodes().forEach(
                    n -> n.getRelationships(ERelations.RELATED, Direction.OUTGOING).forEach(
                            r -> relList.add(new RelationshipModel(r)))
            );

            tx.success();
        } catch (Exception e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }
        return Response.ok(relList).build();
    }

    /**
     * Returns a list of all readings in a tradition
     *
     * @param tradId
     *            the id of the tradition
     * @return the list of readings in json format on success or an ERROR in
     *         JSON format
     */
    @GET
    @Path("{tradId}/readings")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllReadings(@PathParam("tradId") String tradId) {
        Node startNode = DatabaseService.getStartNode(tradId, db);
        if (startNode == null) {
            return Response.status(Status.NOT_FOUND)
                    .entity("Could not find tradition with this id").build();
        }

        ArrayList<ReadingModel> readingModels = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            db.traversalDescription().depthFirst()
                    .relationships(ERelations.SEQUENCE, Direction.OUTGOING)
                    .evaluator(Evaluators.all())
                    .uniqueness(Uniqueness.NODE_GLOBAL).traverse(startNode)
                    .nodes().forEach(node -> readingModels.add(new ReadingModel(node)));
            tx.success();
        } catch (Exception e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }
        return Response.ok(readingModels).build();
    }

    /**
     * Get all readings which have the same text and the same rank between given
     * ranks
     *
     * @param tradId
     *            the id of the tradition in which to look for identical
     *            readings
     * @param startRank
     *            the rank from where to start the search
     * @param endRank
     *            the end rank of the search range
     * @return a list of lists as a json ok response: each list contain
     *         identical readings on success or an ERROR in JSON format
     */
    // TODO refactor all these traversals somewhere!
    @GET
    @Path("{tradId}/identicalreadings/{startRank}/{endRank}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getIdenticalReadings(@PathParam("tradId") String tradId,
                                         @PathParam("startRank") long startRank,
                                         @PathParam("endRank") long endRank) {
        Node startNode = DatabaseService.getStartNode(tradId, db);
        if (startNode == null) {
            return Response.status(Status.NOT_FOUND)
                    .entity("Could not find tradition with this id").build();
        }

        ArrayList<List<ReadingModel>> identicalReadings;
        try {
            ArrayList<ReadingModel> readingModels =
                    getAllReadingsFromTraditionBetweenRanks(startNode, startRank, endRank);
            identicalReadings = identifyIdenticalReadings(readingModels, startRank, endRank);
        } catch (Exception e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }

        Boolean isEmpty = true;
        for (List<ReadingModel> list : identicalReadings) {
            if (list.size() > 0) {
                isEmpty = false;
                break;
            }
        }
        if (isEmpty)
            return Response.status(Status.NOT_FOUND)
                    .entity("no identical readings were found")
                    .build();

        return Response.ok(identicalReadings).build();
    }

    // Retrieve all readings of a tradition between two ranks as Nodes
    private ArrayList<Node> getReadingsBetweenRanks(long startRank, long endRank, Node startNode) {
        ArrayList<Node> readings = new ArrayList<>();

        Evaluator e = path -> {
            Integer rank = Integer.parseInt(path.endNode().getProperty("rank").toString());
            if( rank > endRank )
                return Evaluation.EXCLUDE_AND_PRUNE;
            if ( rank < startRank)
                return Evaluation.EXCLUDE_AND_CONTINUE;
            return Evaluation.INCLUDE_AND_CONTINUE;
        };
        try (Transaction tx = db.beginTx()) {
            db.traversalDescription().depthFirst()
                    .relationships(ERelations.SEQUENCE, Direction.OUTGOING)
                    .evaluator(e).uniqueness(Uniqueness.NODE_GLOBAL)
                    .traverse(startNode).nodes().forEach(readings::add);
            tx.success();
        }
        return readings;
    }
    // Retrieve all readings of a tradition between two ranks as ReadingModels
    private ArrayList<ReadingModel> getAllReadingsFromTraditionBetweenRanks(
            Node startNode, long startRank, long endRank) {
        ArrayList<ReadingModel> readingModels = new ArrayList<>();
        getReadingsBetweenRanks(startRank, endRank, startNode)
                .forEach(x -> readingModels.add(new ReadingModel(x)));
        Collections.sort(readingModels);
        return readingModels;
    }

    // Gets identical readings in a tradition between the given ranks
    private ArrayList<List<ReadingModel>> identifyIdenticalReadings(
            ArrayList<ReadingModel> readingModels, long startRank, long endRank) {
        ArrayList<List<ReadingModel>> identicalReadingsList = new ArrayList<>();

        for (int i = 0; i <= readingModels.size() - 2; i++)
            while (Objects.equals(readingModels.get(i).getRank(), readingModels.get(i + 1)
                    .getRank()) && i + 1 < readingModels.size()) {
                ArrayList<ReadingModel> identicalReadings = new ArrayList<>();

                if (readingModels.get(i).getText()
                        .equals(readingModels.get(i + 1).getText())
                        && readingModels.get(i).getRank() < endRank
                        && readingModels.get(i).getRank() > startRank) {
                    identicalReadings.add(readingModels.get(i));
                    identicalReadings.add(readingModels.get(i + 1));
                }
                identicalReadingsList.add(identicalReadings);
                i++;
            }
        return identicalReadingsList;
    }

    /**
     * Returns a list of a list of readingModels with could be one the same rank
     * without problems
     * TODO use AlignmentTraverse for this...?
     *
     * @param tradId - the tradition to query
     * @param startRank - where to start
     * @param endRank - where to end
     * @return list of readings that could be at the same rank in JSON format or
     *         an ERROR in JSON format
     */
    @GET
    @Path("{tradId}/mergeablereadings/{startRank}/{endRank}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCouldBeIdenticalReadings(
            @PathParam("tradId") String tradId,
            @PathParam("startRank") long startRank,
            @PathParam("endRank") long endRank) {
        Node startNode = DatabaseService.getStartNode(tradId, db);
        if (startNode == null) {
            return Response.status(Status.NOT_FOUND)
                    .entity("Could not find tradition with this id").build();
        }

        ArrayList<ArrayList<ReadingModel>> couldBeIdenticalReadings;
        try (Transaction tx = db.beginTx()) {
            ArrayList<Node> questionedReadings = getReadingsBetweenRanks(
                    startRank, endRank, startNode);

            couldBeIdenticalReadings = getCouldBeIdenticalAsList(questionedReadings);
            tx.success();
        } catch (Exception e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }
        if (couldBeIdenticalReadings.size() == 0)
            return Response.status(Status.NOT_FOUND)
                    .entity("no identical readings were found")
                    .build();

        return Response.ok(couldBeIdenticalReadings).build();
    }

    /**
     * Makes separate lists for every group of readings with identical text and
     * different ranks and send the list for further test
     *
     * @param questionedReadings -
     * @return list of lists of identical readings
     */
    private ArrayList<ArrayList<ReadingModel>> getCouldBeIdenticalAsList(
            ArrayList<Node> questionedReadings) {

        ArrayList<ArrayList<ReadingModel>> couldBeIdenticalReadings = new ArrayList<>();

        for (Node nodeA : questionedReadings) {
            ArrayList<Node> sameText = new ArrayList<>();
            for (Node nodeB : questionedReadings)
                if (!nodeA.equals(nodeB)
                        && nodeA.getProperty("text").toString().equals(nodeB.getProperty("text").toString())
                        && !nodeA.getProperty("rank").toString().equals(nodeB.getProperty("rank").toString())) {
                    sameText.add(nodeB);
                    sameText.add(nodeA);
                }
            if (sameText.size() > 0) {
                couldBeIdenticalCheck(sameText, couldBeIdenticalReadings);
            }
        }
        return couldBeIdenticalReadings;
    }

    /**
     * Adds all the words that could be on the same rank to the result list
     *
     * @param sameText -
     * @param couldBeIdenticalReadings -
     */
    private void couldBeIdenticalCheck(ArrayList<Node> sameText,
                                       ArrayList<ArrayList<ReadingModel>> couldBeIdenticalReadings) {

        Node biggerRankNode;
        Node smallerRankNode;
        long biggerRank;
        long smallerRank;
        long rank;
        boolean gotOne;

        ArrayList<ReadingModel> couldBeIdentical = new ArrayList<>();

        for (int i = 0; i < sameText.size() - 1; i++) {
            long rankA = (long) sameText.get(i).getProperty("rank");
            long rankB = (long) sameText.get(i + 1).getProperty("rank");

            if (rankA < rankB) {
                biggerRankNode = sameText.get(i + 1);
                smallerRankNode = sameText.get(i);
                smallerRank = rankA;
                biggerRank = rankB;
            } else {
                biggerRankNode = sameText.get(i);
                smallerRankNode = sameText.get(i + 1);
                smallerRank = rankB;
                biggerRank = rankA;
            }

            gotOne = false;
            Iterable<Relationship> rels = smallerRankNode
                    .getRelationships(Direction.OUTGOING, ERelations.SEQUENCE);

            for (Relationship rel : rels) {
                rank = (long) rel.getEndNode().getProperty("rank");
                if (rank <= biggerRank) {
                    gotOne = true;
                    break;
                }
            }

            if (gotOne) {
                gotOne = false;

                Iterable<Relationship> rels2 = biggerRankNode
                        .getRelationships(Direction.INCOMING, ERelations.SEQUENCE);

                for (Relationship rel : rels2) {
                    rank = (long) rel.getStartNode().getProperty("rank");
                    if (rank >= smallerRank) {
                        gotOne = true;
                        break;
                    }
                }
            }
            if (!gotOne) {
                if (!couldBeIdentical
                        .contains(new ReadingModel(smallerRankNode))) {
                    couldBeIdentical.add(new ReadingModel(smallerRankNode));
                }
                if (!couldBeIdentical
                        .contains(new ReadingModel(biggerRankNode))) {
                    couldBeIdentical.add(new ReadingModel(biggerRankNode));
                }
            }
            if (couldBeIdentical.size() > 0) {
                couldBeIdenticalReadings.add(couldBeIdentical);
            }
        }
    }

    /******************************
     * Tradition-specific calls
     */

    /**
     * Changes the metadata of the tradition.
     *
     * @param tradition
     *            in JSON Format
     * @return OK and information about the tradition in JSON on success or an
     *         ERROR in JSON format
     */
    @POST
    @Path("{tradId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response changeTraditionMetadata(TraditionModel tradition,
            @PathParam("tradId") String witnessId) {

        if (!DatabaseService.userExists(tradition.getOwnerId(), db)) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity("Error: A user with this id does not exist")
                    .build();
        }

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute("match (witnessId:TRADITION {id:'" + witnessId
                    + "'}) return witnessId");
            Iterator<Node> nodes = result.columnAs("witnessId");

            if (nodes.hasNext()) {
                // Remove the old ownership
                String removeRelationQuery = "MATCH (tradition:TRADITION {id: '" + witnessId + "'}) "
                        + "MATCH tradition<-[r:OWNS_TRADITION]-(:USER) DELETE r";
                result = db.execute(removeRelationQuery);
                System.out.println(result.toString());

                // Add the new ownership
                String createNewRelationQuery = "MATCH(user:USER {id:'" + tradition.getOwnerId()
                        + "'}) " + "MATCH(tradition: TRADITION {id:'" + witnessId + "'}) "
                        + "SET tradition.name = '" + tradition.getName() + "' "
                        + "SET tradition.public = '" + tradition.getIsPublic() + "' "
                        + "CREATE (tradition)<-[r:OWNS_TRADITION]-(user) RETURN r, tradition";
                result = db.execute(createNewRelationQuery);
                System.out.println(result.toString());

            } else {
                // Tradition not found
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Tradition not found")
                        .build();
            }

            tx.success();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
        return Response.ok(tradition).build();
    }

    /**
     * Gets a list of all the complete traditions in the database.
     *
     * @return Http Response 200 and a list of tradition models in JSON on
     *         success or Http Response 500
     */
    @GET
    @Path("all")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllTraditions() {
        List<TraditionModel> traditionList = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {

            Result result = db.execute("match (u:USER)-[:OWNS_TRADITION]->(n:TRADITION) return n");
            Iterator<Node> traditions = result.columnAs("n");
            while(traditions.hasNext())
            {
                Node trad = traditions.next();
                TraditionModel tradModel = new TraditionModel();
                if(trad.hasProperty("id"))
                    tradModel.setId(trad.getProperty("id").toString());
                if(trad.hasProperty("name"))
                    tradModel.setName(trad.getProperty("name").toString());
                traditionList.add(tradModel);
            }
            tx.success();
        } catch (Exception e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }
        return Response.ok(traditionList).build();
    }

    /**
     * Helper method for getting the tradition node with a given tradition id
     *
     * @param tradId ID of the tradition to look up
     * @param engine the graph database to query
     * @return the root tradition node
     */
    private Node getTraditionNode(String tradId, GraphDatabaseService engine) {
        Result result = engine.execute("match (n:TRADITION {id: '" + tradId + "'}) return n");
        Iterator<Node> nodes = result.columnAs("n");

        if (nodes.hasNext()) {
            return nodes.next();
        }
        return null;
    }

    /**
     * Returns GraphML file from specified tradition owned by user
     *
     * @param tradId  ID of the tradition to look up
     * @return XML data
     */
    @GET
    @Path("{tradId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTradition(@PathParam("tradId") String tradId) {
        Neo4JToGraphMLParser parser = new Neo4JToGraphMLParser();
        return parser.parseNeo4J(tradId);
    }

    /**
     * Removes a complete tradition
     *
     * @param tradId ID of the tradition to delete
     * @return http response
     */
    @DELETE
    @Path("{tradId}")
    public Response deleteTraditionById(@PathParam("tradId") String tradId) {
        Node foundTradition = DatabaseService.getTraditionNode(tradId, db);
        if (foundTradition != null) {
            try (Transaction tx = db.beginTx()) {
                /*
                 * Find all the nodes and relations to remove
                 */
                Set<Relationship> removableRelations = new HashSet<>();
                Set<Node> removableNodes = new HashSet<>();
                db.traversalDescription()
                        .depthFirst()
                        .relationships(ERelations.SEQUENCE, Direction.OUTGOING)
                        .relationships(ERelations.COLLATION, Direction.OUTGOING)
                        .relationships(ERelations.LEMMA_TEXT, Direction.OUTGOING)
                        .relationships(ERelations.HAS_END, Direction.OUTGOING)
                        .relationships(ERelations.HAS_WITNESS, Direction.OUTGOING)
                        .relationships(ERelations.HAS_STEMMA, Direction.OUTGOING)
                        .relationships(ERelations.HAS_ARCHETYPE, Direction.OUTGOING)
                        .relationships(ERelations.TRANSMITTED, Direction.OUTGOING)
                        .relationships(ERelations.RELATED, Direction.OUTGOING)
                        .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
                        .traverse(foundTradition)
                        .nodes().forEach(x -> {
                    x.getRelationships().forEach(removableRelations::add);
                    removableNodes.add(x);
                });

                /*
                 * Remove the nodes and relations
                 */
                removableRelations.forEach(Relationship::delete);
                removableNodes.forEach(Node::delete);
                tx.success();
            } catch (Exception e) {
                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            }
        } else {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity("A tradition with this id was not found!")
                    .build();
        }

        return Response.status(Response.Status.OK).build();
    }

    /**
     * Imports a tradition by given GraphML file and meta data
     *
     * @return Http Response with the id of the imported tradition on success or
     *         an ERROR in JSON format
     * @throws XMLStreamException
     */
    @PUT
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response importGraphMl(@FormDataParam("name") String name,
                                  @FormDataParam("language") String language,
                                  @FormDataParam("public") String is_public,
                                  @FormDataParam("userId") String userId,
                                  @FormDataParam("file") InputStream uploadedInputStream,
                                  @FormDataParam("file") FormDataContentDisposition fileDetail) throws IOException,
            XMLStreamException {



        if (!DatabaseService.userExists(userId, db)) {
            return Response.status(Response.Status.CONFLICT)
                    .entity("Error: No user with this id exists")
                    .build();
        }

        return new GraphMLToNeo4JParser().parseGraphML(uploadedInputStream, userId, name);
    }

    /**
     * Returns DOT file from specified tradition owned by user
     *
     * @param tradId ID of the tradition to export
     * @return XML data
     */
    @GET
    @Path("{tradId}/dot")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDot(@PathParam("tradId") String tradId) {
        if(getTraditionNode(tradId, db) == null)
            return Response.status(Status.NOT_FOUND).entity("No such tradition found").build();

        Neo4JToDotParser parser = new Neo4JToDotParser(db);
        return parser.parseNeo4J(tradId);
    }
}
