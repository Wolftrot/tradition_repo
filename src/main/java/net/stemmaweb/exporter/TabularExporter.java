package net.stemmaweb.exporter;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import net.stemmaweb.model.AlignmentModel;
import net.stemmaweb.model.ComplexReadingModel;
import net.stemmaweb.model.WitnessTokensModel;
import net.stemmaweb.rest.ERelations;
import net.stemmaweb.rest.Nodes;
import net.stemmaweb.rest.Reading;
import net.stemmaweb.rest.Relation;
import net.stemmaweb.rest.Tradition;
import net.stemmaweb.model.ReadingModel;
import net.stemmaweb.model.RelationModel;
import net.stemmaweb.model.SectionModel;
import net.stemmaweb.model.TraditionModel;
import net.stemmaweb.model.VariantModel;
import net.stemmaweb.model.WitnessModel;
import net.stemmaweb.model.VariantListModel;
import net.stemmaweb.model.VariantLocationModel;
import net.stemmaweb.services.ReadingService;
import net.stemmaweb.services.VariantGraphService;

import org.checkerframework.checker.units.qual.h;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.unsafe.impl.batchimport.cache.IntArray;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.ParallelSort.Comparator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import com.sun.xml.txw2.output.IndentingXMLStreamWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A class for writing a graph out to various forms of table: JSON, CSV, Excel,
 * etc.
 */
public class TabularExporter {

    private GraphDatabaseService db;

    public TabularExporter(GraphDatabaseService db) {
        this.db = db;
    }

    public Response exportAsJSON(String tradId, List<String> conflate, List<String> sectionList,
            boolean excludeLayers) {
        ArrayList<Node> traditionSections;
        try {
            traditionSections = getSections(tradId, sectionList);
            if (traditionSections == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }

            return Response.ok(getTraditionAlignment(traditionSections, conflate, excludeLayers),
                    MediaType.APPLICATION_JSON_TYPE).build();
        } catch (TabularExporterException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        } catch (Exception e) {
            return Response.serverError().entity(e.getMessage()).build();
        }
    }

    public Response exportAsTEICat(String tradId, List<String> sectionList, String significant, String excludeType1,
            String excludeNonsense, String combine, String suppressMatching, String baseWitness, List<String> conflate,
            List<String> excWitnesses, boolean excludeLayers) {

        ArrayList<Node> traditionSections;
        try {
            traditionSections = getSections(tradId, sectionList);
            if (traditionSections == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }

            return getCriticalApparatus(tradId, traditionSections, significant, excludeType1,
                    excludeNonsense, combine, suppressMatching, baseWitness, conflate, excWitnesses, excludeLayers);
        } catch (TabularExporterException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }

    }

    public Response exportAsTEICat2(String tradId, List<String> sectionList, String significant, String excludeType1,
            String excludeNonsense, String combine, String suppressMatching, String baseWitness, List<String> conflate,
            List<String> excWitnesses, boolean excludeLayers) {

        ArrayList<Node> traditionSections;
        try {
            traditionSections = getSections(tradId, sectionList);
            if (traditionSections == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }

            return getCriticalApparatus2(tradId, traditionSections, significant, excludeType1,
                    excludeNonsense, combine, suppressMatching, baseWitness, conflate, excWitnesses, excludeLayers);
        } catch (TabularExporterException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }

    }

    public Response exportAsCSV(String tradId, char separator, List<String> conflate, List<String> sectionList,
            boolean excludeLayers) {
        AlignmentModel wholeTradition;
        try {
            wholeTradition = returnFullAlignment(tradId, conflate, sectionList, excludeLayers);
        } catch (TabularExporterException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        } catch (Exception e) {
            return Response.serverError().entity(e.getMessage()).build();
        }
        if (wholeTradition == null)
            return Response.status(Response.Status.NOT_FOUND).build();

        // Got this far? Turn it into CSV.
        // The CSV will go into a string that we can return.
        StringWriter sw = new StringWriter();
        ICSVWriter writer = new CSVWriterBuilder(sw)
                .withSeparator(separator)
                .build();

        // First write out the witness list
        writer.writeNext(wholeTradition.getAlignment().stream()
                .map(WitnessTokensModel::constructSigil).toArray(String[]::new));

        // Now write out the normal_form or text for the reading in each "row"
        for (int i = 0; i < wholeTradition.getLength(); i++) {
            AtomicInteger ai = new AtomicInteger(i);
            writer.writeNext(wholeTradition.getAlignment().stream()
                    .map(x -> {
                        ReadingModel rm = x.getTokens().get(ai.get());
                        return rm == null ? null : rm.normalized();
                    }).toArray(String[]::new));
        }

        // Close off the CSV writer and return
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }
        return Response.ok(sw.toString(), MediaType.TEXT_PLAIN_TYPE).build();
    }

    public Response exportAsCharMatrix(String tradId, int maxVars, List<String> conflate, List<String> sectionList,
            boolean excludeLayers) {
        AlignmentModel wholeTradition;
        try {
            wholeTradition = returnFullAlignment(tradId, conflate, sectionList, excludeLayers);
            if (wholeTradition == null)
                return Response.status(Response.Status.NOT_FOUND).build();
        } catch (TabularExporterException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        } catch (Exception e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        // We will count on the order of the witness columns remaining constant.
        List<String> witnessSigla = wholeTradition.getAlignment().stream()
                .map(WitnessTokensModel::constructSigil).collect(Collectors.toList());
        // Collect the character rows as they are built for each witness.
        HashMap<String, StringBuilder> witnessRows = new HashMap<>();
        for (String sigil : witnessSigla)
            witnessRows.put(sigil, new StringBuilder());
        // Go rank by rank through all the lists of tokens, converting them into chars
        int totalLength = 0;
        for (int i = 0; i < wholeTradition.getLength(); i++) {
            AtomicInteger ai = new AtomicInteger(i);
            List<ReadingModel> row = wholeTradition.getAlignment().stream()
                    .map(x -> x.getTokens().get(ai.get())).collect(Collectors.toList());
            // Make reading-to-character lookup
            HashMap<String, Character> charMap = new HashMap<>();
            char curr = 'A';
            boolean row_has_null = false;
            boolean row_has_lacuna = false;
            for (ReadingModel rm : row) {
                if (rm == null) {
                    row_has_null = true;
                    continue;
                } else if (rm.getIs_lacuna()) {
                    row_has_lacuna = true;
                    continue;
                }
                if (!charMap.containsKey(rm.getId())) {
                    charMap.put(rm.getId(), curr);
                    curr++;
                }
            }
            // Skip rows that don't diverge
            if (curr == 'B' && !row_has_null && !row_has_lacuna)
                continue;
            // Check that we aren't over the max-character limit
            if (curr > 'A' + maxVars || row_has_null && curr > 'A' + (maxVars - 1))
                continue;

            // Employ it
            totalLength++;
            for (int w = 0; w < witnessSigla.size(); w++) {
                StringBuilder ourRow = witnessRows.get(witnessSigla.get(w));
                ReadingModel ourReading = row.get(w);
                if (ourReading == null) {
                    ourRow.append('X');
                    curr++; // Count this in our maximum of eight characters
                } else if (ourReading.getIs_lacuna())
                    ourRow.append('?');
                else
                    ourRow.append(charMap.get(row.get(w).getId()));
            }
        }
        // Now let's build the whole matrix.
        StringBuilder charMatrix = new StringBuilder();
        charMatrix.append(String.format("\t%d\t%d\n", wholeTradition.getAlignment().size(), totalLength));
        for (String sigil : witnessSigla) {
            charMatrix.append(String.format("%-10s", shortenSigil(sigil)));
            charMatrix.append(witnessRows.get(sigil));
            charMatrix.append("\n");
        }

        return Response.ok(charMatrix.toString()).build();
    }

    private AlignmentModel returnFullAlignment(String tradId, List<String> conflate, List<String> sectionList,
            boolean excludeLayers)
            throws Exception {
        ArrayList<Node> traditionSections = getSections(tradId, sectionList);
        if (traditionSections == null)
            return null;
        return getTraditionAlignment(traditionSections, conflate, excludeLayers);
    }

    private static String shortenSigil(String sigil) {
        String shortened = sigil.replaceAll("\\s+", "_")
                .replaceAll("\\W+", "");
        if (shortened.length() > 10)
            shortened = shortened.substring(0, 10);
        return shortened;
    }

    private ArrayList<Node> getSections(String tradId, List<String> sectionList)
            throws TabularExporterException {
        ArrayList<Node> traditionSections = VariantGraphService.getSectionNodes(tradId, db);
        // Does the tradition exist in the first place?
        if (traditionSections == null)
            return null;

        // Are we requesting all sections?
        if (sectionList.size() == 0)
            return traditionSections;

        // Do the real work
        ArrayList<Node> collectedSections = new ArrayList<>();
        for (String sectionId : sectionList) {
            try (Transaction tx = db.beginTx()) {
                collectedSections.add(db.getNodeById(Long.parseLong(sectionId)));
                tx.success();
            } catch (NotFoundException e) {
                throw new TabularExporterException("Section " + sectionId + " not found in tradition");
            }
        }
        return collectedSections;
    }

    private AlignmentModel getTraditionAlignment(ArrayList<Node> traditionSections, List<String> collapseRelated,
            boolean excludeLayers)
            throws Exception {
        // Make a new alignment model that has a column for every witness layer across
        // the requested sections.

        // For each section, get the model. Keep track of which layers in which
        // witnesses we have
        // seen with a set.
        HashSet<String> allWitnesses = new HashSet<>();
        ArrayList<AlignmentModel> tables = new ArrayList<>();
        int length = 0;
        for (Node sectionNode : traditionSections) {
            if (collapseRelated != null)
                VariantGraphService.normalizeGraph(sectionNode, collapseRelated);
            AlignmentModel asJson = new AlignmentModel(sectionNode, excludeLayers);
            if (collapseRelated != null)
                VariantGraphService.clearNormalization(sectionNode);
            // Save the alignment to our tables list
            tables.add(asJson);
            length += asJson.getLength();
            // Save the witness -> column mapping to our map
            for (WitnessTokensModel witRecord : asJson.getAlignment()) {
                allWitnesses.add(witRecord.constructSigil());
            }
        }

        // Now make an alignment model containing all witness layers present in
        // allWitnesses, filling in
        // if necessary either nulls or the base witness per witness layer, per section.
        AlignmentModel wholeTradition = new AlignmentModel();
        List<String> sortedWits = new ArrayList<>(allWitnesses);
        Collections.sort(sortedWits);
        for (String sigil : sortedWits) {
            String[] parsed = WitnessTokensModel.parseSigil(sigil);

            // Set up the tradition-spanning witness token model for this witness
            WitnessTokensModel wholeWitness = new WitnessTokensModel();
            wholeWitness.setWitness(parsed[0]);
            if (parsed[1] != null)
                wholeWitness.setLayer(parsed[1]);
            wholeWitness.setTokens(new ArrayList<>());
            // Now fill in tokens from each section in turn.
            for (AlignmentModel aSection : tables) {
                // Find the WitnessTokensModel corresponding to wit, if it exists
                Optional<WitnessTokensModel> thisWitness = aSection.getAlignment().stream()
                        .filter(x -> x.constructSigil().equals(sigil)).findFirst();
                if (!thisWitness.isPresent()) {
                    // Try again for the base witness
                    thisWitness = aSection.getAlignment().stream()
                            .filter(x -> x.getWitness().equals(parsed[0]) && !x.hasLayer()).findFirst();
                }

                if (thisWitness.isPresent()) {
                    WitnessTokensModel witcolumn = thisWitness.get();
                    wholeWitness.getTokens().addAll(witcolumn.getTokens());
                    assert (witcolumn.getTokens().size() == aSection.getLength());
                } else {
                    // Add a bunch of nulls
                    wholeWitness.getTokens()
                            .addAll(new ArrayList<>(Collections.nCopies((int) aSection.getLength(), null)));
                }
            }
            // Add the WitnessTokensModel to the new AlignmentModel.
            wholeTradition.addWitness(wholeWitness);
        }
        // Record the length of the whole alignment
        wholeTradition.setLength(length);
        return wholeTradition;
    }

    private Response getCriticalApparatus(String tradId, ArrayList<Node> traditionSections, String significant,
            String excludeType1,
            String excludeNonsense, String combine, String suppressMatching, String baseWitness, List<String> conflate,
            List<String> excWitnesses, boolean excludeLayers) throws Exception {

        try (Transaction tx = db.beginTx()) {

            // XML INDENTED WRITER
            StringWriter result = new StringWriter();
            XMLOutputFactory output = XMLOutputFactory.newInstance();
            XMLStreamWriter writer;
            try {
                XMLStreamWriter xmlStreamWriter = output.createXMLStreamWriter(result);
                writer = new IndentingXMLStreamWriter(xmlStreamWriter);

            } catch (XMLStreamException e) {
                e.printStackTrace();
                return Response.serverError().build();
            }
            writer.writeStartDocument();

            // ROOT ELEMENT
            // writer.writeEmptyElement("?xml");
            // writer.writeAttribute("version", "1.0");
            // writer.writeAttribute("encoding", "UTF-8");
            writer.writeStartElement("TEI");
            writer.setDefaultNamespace("http://www.tei-c.org/ns/1.0");
            writer.writeDefaultNamespace("http://www.tei-c.org/ns/1.0");
            writer.writeAttribute("xml:lang", "en");
            writer.writeAttribute("demo-purposes", "dev-tei-parallel-segmentation-1"); // TO REMOVE

            // TEI XML HEADER
            writer.writeStartElement("teiHeader");
            writer.writeStartElement("fileDesc");
            writer.writeStartElement("titleStmt");
            writer.writeStartElement("title");
            if (tradId != null) {
                // get tradition node
                Node traditionNode = VariantGraphService.getTraditionNode(tradId, db);
                String traditionName = traditionNode.getProperty("name").toString();

                if (traditionNode.hasProperty("language")) {
                    String traditionLanguage = traditionNode.getProperty("language").toString();
                    String tradLanAbbr = traditionLanguage.substring(0, 2);
                    writer.writeAttribute("xml:lang", tradLanAbbr);
                }

                writer.writeCharacters(traditionName);
                writer.writeEndElement(); // title
                writer.writeEndElement(); // titleStmt
            }

            // WITNESS LIST
            writer.writeStartElement("sourceDesc");
            writer.writeStartElement("listWit");

            Node traditionNode = VariantGraphService.getTraditionNode(tradId, db);
            TraditionModel tradition = new TraditionModel(traditionNode);
            ArrayList<String> witnesses = tradition.getWitnesses();

            // get witness nodes from tradition list of witnesses
            for (String wit : witnesses) {

                writer.writeStartElement("witness");
                writer.writeAttribute("xml:id", wit);

                writer.writeStartElement("abbr");
                writer.writeAttribute("type", "sigil");
                writer.writeCharacters(wit);
                writer.writeEndElement(); // abbr

                writer.writeEndElement(); // witness

            }

            writer.writeEndElement(); // listWit
            writer.writeEndElement(); // sourceDesc
            writer.writeEndElement(); // fileDesc

            // ENCONDING DESCRIPTION
            writer.writeStartElement("encodingDesc");

            // VARIANT ENCODING
            writer.writeEmptyElement("variantEnconding");
            writer.writeAttribute("method", "parralel-segmentation");
            writer.writeAttribute("location", "internal");

            // PROFILE DESCRIPTION
            // No info available

            // REVISION DESCRIPTION
            // No info available

            // CLOSE HEADER
            writer.writeEndElement(); // encodingDesc
            writer.writeEndElement(); // teiHeader

            // START CORPUS
            writer.writeStartElement("text");
            writer.writeStartElement("body");
            for (Node sectionNode : traditionSections) {

                // GET START NODE OF SECTION
                String sectId = Long.toString(sectionNode.getId());
                Node startNode = VariantGraphService.getStartNode(sectId, db);
                if (startNode == null)
                    throw new Exception("Section " + sectId + " has no start node");

                // NO RELATIONSHIP OUTPUT -> I DROPPED THIS POSSIBILITY
                // GET RELATIONSHIPS WITH NODES
                ArrayList<RelationModel> relList = new ArrayList<>();
                try (Transaction txSectionNode = db.beginTx()) {
                    db.traversalDescription().depthFirst()
                            .relationships(ERelations.SEQUENCE, Direction.OUTGOING)
                            .uniqueness(Uniqueness.NODE_GLOBAL)
                            .traverse(startNode).nodes().forEach(
                                    n -> n.getRelationships(ERelations.RELATED, Direction.OUTGOING).forEach(
                                            r -> relList.add(new RelationModel(r, true))));

                    txSectionNode.success();
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }

                // write down all realtionships
                writer.writeStartElement("Relationshiplist");
                for (RelationModel rel : relList) {
                    writer.writeStartElement("relation");
                    writer.writeAttribute("id", rel.getId());
                    writer.writeAttribute("source", rel.getSource()); // reading id
                    writer.writeAttribute("target", rel.getTarget()); // reading id
                    writer.writeAttribute("hypernodeSource", rel.getHSource()); // hypernode id
                    writer.writeAttribute("hypernodeTarget", rel.getHTarget()); // hypernode id
                    writer.writeAttribute("type", rel.getType());
                    writer.writeEndElement(); // relation

                    // HYPERNODE APPARATUS
                    writer.writeStartElement("app");
                    writer.writeAttribute("is_hyperrelation", Boolean.toString(rel.getIs_hyperrelation()));
                    writer.writeAttribute("type", rel.getType());

                    // GET BOTH HYPERNODEs
                    Node hSource = db.getNodeById(Long.parseLong(rel.getHSource()));
                    ComplexReadingModel hSourceCRM = new ComplexReadingModel(hSource);
                    List<ComplexReadingModel> hSourceReadings = hSourceCRM.getComponents();

                    // Get all the witnesses of the readings composing the hypernode
                    // and filter the the common witnesses between the readings
                    List<String> hSourceCommonWitnesses = hSourceReadings
                            .stream()
                            .flatMap(x -> x.getReading().getWitnesses().stream())
                            .filter(y -> hSourceReadings.stream()
                                    .allMatch(z -> z.getReading().getWitnesses().contains(y)))
                            .distinct()
                            .collect(Collectors.toList());

                    ArrayList<String> hSourceSigList = new ArrayList<>();
                    for (String wit : hSourceCommonWitnesses)
                        hSourceSigList.add("#" + wit);
                    Collections.sort(hSourceSigList);
                    String hSourceWitnessList = String.join(" ", hSourceSigList);

                    writer.writeStartElement("rdg");

                    if (hSourceCRM.getId() != null && !hSourceCRM.getId().isEmpty()) {
                        writer.writeAttribute("xml:id", hSourceCRM.getId());
                    }

                    if (hSourceWitnessList != null && !hSourceWitnessList.isEmpty()) {
                        writer.writeAttribute("wit", hSourceWitnessList);
                    }

                    List<ComplexReadingModel> hSourceSortedHypernodeReadings = hSourceReadings
                            .stream()
                            .sorted(
                                    java.util.Comparator.comparing(
                                            x -> x.getReading().getRank()))
                            .collect(Collectors.toList());

                    for (ComplexReadingModel hsrm : hSourceSortedHypernodeReadings) {
                        writer.writeCharacters(hsrm.getReading().getText());
                    }

                    writer.writeEndElement(); // rdg

                    Node hTarget = db.getNodeById(Long.parseLong(rel.getHTarget()));
                    ComplexReadingModel hTargetCRM = new ComplexReadingModel(hTarget);
                    List<ComplexReadingModel> hTargetReadings = hTargetCRM.getComponents();

                    // Get all the witnesses of the readings composing the hypernode
                    // and filter the the common witnesses between the readings
                    List<String> hTargetCommonWitnesses = hTargetReadings
                            .stream()
                            .flatMap(x -> x.getReading().getWitnesses().stream())
                            .filter(y -> hTargetReadings.stream()
                                    .allMatch(z -> z.getReading().getWitnesses().contains(y)))
                            .distinct()
                            .collect(Collectors.toList());

                    // List<String> readingsWitnesses =
                    // hTargetReadings.get(0).getReading().getWitnesses();
                    ArrayList<String> hTargetSigList = new ArrayList<>();
                    for (String wit : hTargetCommonWitnesses)
                        hTargetSigList.add("#" + wit);
                    Collections.sort(hTargetSigList);
                    String hTargetWitnessList = String.join(" ", hTargetSigList);

                    writer.writeStartElement("rdg");
                    if (hTargetCRM.getId() != null && !hTargetCRM.getId().isEmpty()) {
                        writer.writeAttribute("xml:id", hTargetCRM.getId());
                    }

                    if (hTargetWitnessList != null && !hTargetWitnessList.isEmpty()) {
                        writer.writeAttribute("wit", hTargetWitnessList);
                    }

                    if (rel.getType() != null && !rel.getType().isEmpty()) {
                        writer.writeAttribute("cause", rel.getType());
                    }
                    // sort hypernodeReadings by rank
                    // hypernodeReadings.stream().forEachOrdered(x -> {
                    // x.getReading().getRank();
                    // });

                    List<ComplexReadingModel> hTargetSortedHypernodeReadings = hTargetReadings
                            .stream()
                            .sorted(
                                    java.util.Comparator.comparing(x -> x.getReading().getRank()

                                    )).collect(Collectors.toList());

                    // List<ComplexReadingModel> sortedHypernodeReadings =
                    // hypernodeReadings.stream()
                    // .sorted(Comparator.comparing(x -> x.getReading().getRank()))
                    // .collect(Collectors.toList());

                    for (ComplexReadingModel htrm : hTargetSortedHypernodeReadings) {
                        writer.writeCharacters(htrm.getReading().getText());
                    }

                    writer.writeEndElement(); // rdg

                    // ANNOTATION FOR RELATION
                    if (rel.getAnnotation() != null && !rel.getAnnotation().isEmpty()) {
                        writer.writeStartElement("note");
                        writer.writeCharacters(rel.getAnnotation());
                        writer.writeEndElement(); // note
                    }

                    writer.writeEndElement(); // app

                }
                writer.writeEndElement(); // relationshiplist

                // GET HYPERREADING NODES
                ArrayList<ComplexReadingModel> complexReadingModels = new ArrayList<>();
                Set<Node> sectionNodes = VariantGraphService.returnTraditionSection(startNode).nodes()
                        .stream()
                        .filter(x -> x.hasLabel(Label.label("HYPERREADING"))).collect(Collectors.toSet());
                sectionNodes.forEach(x -> complexReadingModels.add(new ComplexReadingModel(x)));

                writer.writeStartElement("hypernodes");
                writer.writeStartElement("p");

                writer.writeStartElement("app");
                for (ComplexReadingModel crm : complexReadingModels) {

                    List<ComplexReadingModel> hypernodeReadings = crm.getComponents();

                    // Get all the witnesses of the readings composing the hypernode
                    // and filter the the common witnesses between the readings
                    List<String> commonWitnesses = hypernodeReadings
                            .stream()
                            .flatMap(x -> x.getReading().getWitnesses().stream())
                            .filter(y -> hypernodeReadings.stream()
                                    .allMatch(z -> z.getReading().getWitnesses().contains(y)))
                            .distinct()
                            .collect(Collectors.toList());

                    // List<String> readingsWitnesses =
                    // hypernodeReadings.get(0).getReading().getWitnesses();
                    ArrayList<String> sigList = new ArrayList<>();
                    for (String wit : commonWitnesses)
                        sigList.add("#" + wit);
                    Collections.sort(sigList);
                    String witnessList = String.join(" ", sigList);

                    writer.writeStartElement("rdg");

                    if (witnessList != null && !witnessList.isEmpty()) {
                        writer.writeAttribute("wit", witnessList);
                    }

                    List<ComplexReadingModel> sortedHypernodeReadings = hypernodeReadings
                            .stream()
                            .sorted(
                                    java.util.Comparator.comparing(x -> x.getReading().getRank()

                                    )).collect(Collectors.toList());

                    for (ComplexReadingModel hrm : sortedHypernodeReadings) {

                        if (hrm.getReading() != null) {

                            writer.writeCharacters(hrm.getReading().getText());

                        }
                    }

                    writer.writeEndElement(); // rdg

                }
                writer.writeEndElement(); // app

                writer.writeEndElement(); // p
                writer.writeEndElement(); // hypernodes

                writer.writeStartElement("div");
                writer.writeStartElement("p");
                VariantListModel vlm = new VariantListModel(
                        sectionNode, baseWitness, excWitnesses, conflate, suppressMatching,
                        !excludeNonsense.equals("no"), !excludeType1.equals("no"), significant, !combine.equals("no"));
                vlm.getBaseChain().forEach(
                        x -> {
                            List<VariantLocationModel> variantLocationFound = vlm.getVariantlist().stream()
                                    .filter(y -> y.getRankIndex().equals(x.getRank())).collect(Collectors.toList());

                            // If the reading has variants, build up the critical apparatus
                            if (!variantLocationFound.isEmpty()) {
                                try {

                                    // OPEN CRITICAL APPARATUS
                                    writer.writeStartElement("app");
                                    writer.writeStartElement("lem"); // OPEN LEMMA

                                    // getWitnessList of lemma
                                    List<String> lem_wits = x.getWitnesses();
                                    ArrayList<String> sigList = new ArrayList<>();
                                    for (String l : lem_wits)
                                        sigList.add("#" + l);
                                    Collections.sort(sigList);
                                    String witnessList = String.join(" ", sigList);
                                    writer.writeAttribute("wit", witnessList);

                                    // getRank
                                    writer.writeAttribute("rank", Long.toString(x.getRank()));

                                    writer.writeCharacters(x.getText()); // write inner text

                                    writer.writeEndElement(); // lem

                                    VariantLocationModel vloc = variantLocationFound.get(0);
                                    for (VariantModel vm : vloc.getVariants()) {

                                        writer.writeStartElement("rdg");

                                        // getWitnessList
                                        Map<String, List<String>> wits = vm.getWitnesses();
                                        ArrayList<String> variantSigList = new ArrayList<>();
                                        for (String l : wits.keySet())
                                            for (String s : wits.get(l))
                                                variantSigList.add(
                                                        l.equals("witnesses") ? "#" + s
                                                                : "#" + String.format("%s (%s)", s, l));
                                        Collections.sort(variantSigList);
                                        String variantWitnessList = String.join(" ", variantSigList);
                                        writer.writeAttribute("wit", variantWitnessList);

                                        // getRanksList
                                        // Empty nodes have no rank and make the export fail
                                        if (vm.getReadings().size() > 0) {

                                            String variantRank = Long.toString(vm.getReadings().get(0).getRank());
                                            writer.writeAttribute("rank", variantRank);
                                        }

                                        String varText = ReadingService.textOfReadings(vm.getReadings(),
                                                vloc.isNormalised(),
                                                false);
                                        writer.writeCharacters(varText);
                                        writer.writeEndElement(); // rdg
                                    }
                                    writer.writeEndElement(); // app
                                    writer.writeCharacters(" ");
                                } catch (XMLStreamException e) {
                                    e.printStackTrace();
                                }
                            }
                            // If the reading has no variants, just write it out
                            else {
                                try {
                                    writer.writeCharacters(x.getText() + " ");
                                } catch (XMLStreamException e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                writer.writeEndElement(); // p
                writer.writeEndElement(); // div
            }

            writer.writeEndElement(); // body
            writer.writeEndElement(); // text
            writer.writeEndElement(); // TEI

            writer.flush();

            return Response.ok(result.toString(), MediaType.APPLICATION_XML).build();
            // MediaType.APPLICATION_JSON_TYPE).build()
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }
    }

    private Response getCriticalApparatus2(String tradId, ArrayList<Node> traditionSections, String significant,
            String excludeType1,
            String excludeNonsense, String combine, String suppressMatching, String baseWitness, List<String> conflate,
            List<String> excWitnesses, boolean excludeLayers) throws Exception {

        try (Transaction tx = db.beginTx()) {

            // XML INDENTED WRITER
            StringWriter result = new StringWriter();
            XMLOutputFactory output = XMLOutputFactory.newInstance();
            XMLStreamWriter writer;
            try {
                XMLStreamWriter xmlStreamWriter = output.createXMLStreamWriter(result);
                writer = new IndentingXMLStreamWriter(xmlStreamWriter);

            } catch (XMLStreamException e) {
                e.printStackTrace();
                return Response.serverError().build();
            }

            // ROOT ELEMENT
            writer.writeStartDocument("UTF-8", "1.0");
            writer.writeStartElement("TEI");
            writer.setDefaultNamespace("http://www.tei-c.org/ns/1.0");
            writer.writeDefaultNamespace("http://www.tei-c.org/ns/1.0");
            writer.writeAttribute("xml:lang", "en");

            // TEI XML HEADER
            writer.writeStartElement("teiHeader");
            writer.writeStartElement("fileDesc");
            writer.writeStartElement("titleStmt");
            writer.writeStartElement("title");
            if (tradId != null) {
                // get tradition node
                Node traditionNode = VariantGraphService.getTraditionNode(tradId, db);
                String traditionName = traditionNode.getProperty("name").toString();

                if (traditionNode.hasProperty("language")) {
                    String traditionLanguage = traditionNode.getProperty("language").toString();
                    String tradLanAbbr = traditionLanguage.substring(0, 2);
                    writer.writeAttribute("xml:lang", tradLanAbbr);
                }

                writer.writeCharacters(traditionName);
                writer.writeEndElement(); // title
                writer.writeEndElement(); // titleStmt
            }

            // WITNESS LIST
            writer.writeStartElement("sourceDesc");
            writer.writeStartElement("listWit");

            Node traditionNode = VariantGraphService.getTraditionNode(tradId, db);
            TraditionModel tradition = new TraditionModel(traditionNode);
            ArrayList<String> witnesses = tradition.getWitnesses();

            // get witness nodes from tradition list of witnesses
            for (String wit : witnesses) {

                writer.writeStartElement("witness");
                writer.writeAttribute("xml:id", wit);

                writer.writeStartElement("abbr");
                writer.writeAttribute("type", "sigil");
                writer.writeCharacters(wit);
                writer.writeEndElement(); // abbr

                writer.writeEndElement(); // witness

            }

            writer.writeEndElement(); // listWit
            writer.writeEndElement(); // sourceDesc
            writer.writeEndElement(); // fileDesc

            // ENCONDING DESCRIPTION
            writer.writeStartElement("encodingDesc");

            // VARIANT ENCODING
            writer.writeEmptyElement("variantEnconding");
            writer.writeAttribute("method", "parralel-segmentation");
            writer.writeAttribute("location", "internal");

            // PROFILE DESCRIPTION
            // No info available

            // REVISION DESCRIPTION
            // No info available

            // CLOSE HEADER
            writer.writeEndElement(); // encodingDesc
            writer.writeEndElement(); // teiHeader

            // START CORPUS
            writer.writeStartElement("text");
            writer.writeStartElement("body");
            for (Node sectionNode : traditionSections) {

                // GET START NODE OF SECTION
                String sectId = Long.toString(sectionNode.getId());
                Node startNode = VariantGraphService.getStartNode(sectId, db);
                if (startNode == null)
                    throw new Exception("Section " + sectId + " has no start node");

                // GET ALL RR RELATIONSHIPS AND CAST THEM INTO EXTENDED CLASS "RELATIONNODES"
                // LIST
                ArrayList<RelationModel> relationshipsList = new ArrayList<>();
                try (Transaction txSectionNode = db.beginTx()) {
                    db.traversalDescription().depthFirst()
                            .relationships(ERelations.SEQUENCE, Direction.OUTGOING)
                            .uniqueness(Uniqueness.NODE_GLOBAL)
                            .traverse(startNode).nodes().forEach(
                                    n -> n.getRelationships(ERelations.RELATED, Direction.OUTGOING)
                                            .forEach(r -> relationshipsList.add(
                                                    new RelationModel(r, true))));

                    txSectionNode.success();
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }

                // FEED A LIST OF RELATION STRUCT STORING NODES FOR FUTURE USE
                // INSTEAD OF TRAVERSING THE GRAPH EACH TIME WE WANT TO COMPARE
                // STANDARD NODES LOOKING FOR HYPERNODES OR RELATIONSHIPS
                ArrayList<RelationStruct> relStructList = new ArrayList<>();
                for (RelationModel rel : relationshipsList) {
                    RelationStruct relStruct = new RelationStruct();
                    relStruct.relationModel = rel;
                    Node hSource = db.getNodeById(Long.parseLong(rel.getHSource()));
                    Node hTarget = db.getNodeById(Long.parseLong(rel.getHTarget()));
                    relStruct.hSource = new ComplexReadingModel(hSource);
                    relStruct.hTarget = new ComplexReadingModel(hTarget);

                    relStruct.hSourceReadings = relStruct.hSource.getComponents().stream()
                            .sorted(java.util.Comparator.comparing(x -> x.getReading().getRank()))
                            .map(x -> x.getReading())
                            .collect(Collectors.toList());
                    relStruct.hTargetReadings = relStruct.hTarget.getComponents().stream()
                            .sorted(java.util.Comparator.comparing(x -> x.getReading().getRank()))
                            .map(x -> x.getReading())
                            .collect(Collectors.toList());

                    Node source = db.getNodeById(Long.parseLong(rel.getSource()));
                    Node target = db.getNodeById(Long.parseLong(rel.getTarget()));
                    relStruct.sourceNode = new ReadingModel(source);
                    relStruct.targetNode = new ReadingModel(target);

                    relStructList.add(relStruct);

                    // // print rel
                    // writer.writeStartElement("relation");
                    // writer.writeAttribute("id", rel.getId());
                    // writer.writeAttribute("source", rel.getSource()); // reading id
                    // writer.writeAttribute("target", rel.getTarget()); // reading id
                    // writer.writeAttribute("hypernodeSource", rel.getHSource()); // hypernode id
                    // writer.writeAttribute("hypernodeTarget", rel.getHTarget()); // hypernode id
                    // writer.writeAttribute("type", rel.getType());
                    // writer.writeAttribute("is_hyperrelation", Boolean.toString(rel.getIs_hyperrelation()));
                    // writer.writeEndElement(); // relation
                }

                // // write down all relationships
                // writer.writeStartElement("Relationshiplist");
                // for (RelationModel relationship : relationshipsList) {
                //     writer.writeStartElement("relation");
                //     writer.writeAttribute("id", relationship.getId());
                //     writer.writeAttribute("source", relationship.getSource()); // reading id
                //     writer.writeAttribute("target", relationship.getTarget()); // reading id
                //     writer.writeAttribute("hypernodeSource", relationship.getHSource()); // hypernode id
                //     writer.writeAttribute("hypernodeTarget", relationship.getHTarget()); // hypernode id
                //     writer.writeAttribute("type", relationship.getType());
                //     writer.writeEndElement(); // relation

                //     // HYPERNODE APPARATUS
                //     writer.writeStartElement("app");
                //     writer.writeAttribute("is_hyperrelation", Boolean.toString(relationship.getIs_hyperrelation()));
                //     writer.writeAttribute("type", relationship.getType());

                //     if (relationship.getIs_hyperrelation()) {
                //         // GET BOTH HYPERNODEs
                //         Node hSource = db.getNodeById(Long.parseLong(relationship.getHSource()));
                //         ComplexReadingModel hSourceCRM = new ComplexReadingModel(hSource);
                //         List<ComplexReadingModel> hSourceReadings = hSourceCRM.getComponents();

                //         // Get all the witnesses of the readings composing the hypernode
                //         // and filter the the common witnesses between the readings
                //         List<String> hSourceCommonWitnesses = hSourceReadings
                //                 .stream()
                //                 .flatMap(x -> x.getReading().getWitnesses().stream())
                //                 .filter(y -> hSourceReadings.stream()
                //                         .allMatch(z -> z.getReading().getWitnesses().contains(y)))
                //                 .distinct()
                //                 .collect(Collectors.toList());

                //         ArrayList<String> hSourceSigList = new ArrayList<>();
                //         for (String wit : hSourceCommonWitnesses)
                //             hSourceSigList.add("#" + wit);
                //         Collections.sort(hSourceSigList);
                //         String hSourceWitnessList = String.join(" ", hSourceSigList);

                //         writer.writeStartElement("rdg");

                //         if (hSourceCRM.getId() != null && !hSourceCRM.getId().isEmpty()) {
                //             writer.writeAttribute("xml:id", hSourceCRM.getId());
                //         }

                //         if (hSourceWitnessList != null && !hSourceWitnessList.isEmpty()) {
                //             writer.writeAttribute("wit", hSourceWitnessList);
                //         }

                //         List<ComplexReadingModel> hSourceSortedHypernodeReadings = hSourceReadings
                //                 .stream()
                //                 .sorted(
                //                         java.util.Comparator.comparing(
                //                                 x -> x.getReading().getRank()))
                //                 .collect(Collectors.toList());

                //         for (ComplexReadingModel hsrm : hSourceSortedHypernodeReadings) {

                //             ReadingModel reading = hsrm.getReading();
                //             writer.writeCharacters(reading.getText());
                //             writer.writeCharacters(" | getID:" + reading.getId());
                //             writer.writeCharacters(" | getRank:" + reading.getRank().toString());
                //             writer.writeCharacters(" | getWitnesses:" +
                //                     reading.getWitnesses().toString());
                //         }

                //         writer.writeEndElement(); // rdg

                //         Node hTarget = db.getNodeById(Long.parseLong(relationship.getHTarget()));
                //         ComplexReadingModel hTargetCRM = new ComplexReadingModel(hTarget);
                //         List<ComplexReadingModel> hTargetReadings = hTargetCRM.getComponents();

                //         // Get all the witnesses of the readings composing the hypernode
                //         // and filter the the common witnesses between the readings
                //         List<String> hTargetCommonWitnesses = hTargetReadings
                //                 .stream()
                //                 .flatMap(x -> x.getReading().getWitnesses().stream())
                //                 .filter(y -> hTargetReadings.stream()
                //                         .allMatch(z -> z.getReading().getWitnesses().contains(y)))
                //                 .distinct()
                //                 .collect(Collectors.toList());

                //         // List<String> readingsWitnesses =
                //         // hTargetReadings.get(0).getReading().getWitnesses();
                //         ArrayList<String> hTargetSigList = new ArrayList<>();
                //         for (String wit : hTargetCommonWitnesses)
                //             hTargetSigList.add("#" + wit);
                //         Collections.sort(hTargetSigList);
                //         String hTargetWitnessList = String.join(" ", hTargetSigList);

                //         writer.writeStartElement("rdg");
                //         if (hTargetCRM.getId() != null && !hTargetCRM.getId().isEmpty()) {
                //             writer.writeAttribute("xml:id", hTargetCRM.getId());
                //         }

                //         if (hTargetWitnessList != null && !hTargetWitnessList.isEmpty()) {
                //             writer.writeAttribute("wit", hTargetWitnessList);
                //         }

                //         if (relationship.getType() != null && !relationship.getType().isEmpty()) {
                //             writer.writeAttribute("cause", relationship.getType());
                //         }
                //         // sort hypernodeReadings by rank
                //         // hypernodeReadings.stream().forEachOrdered(x -> {
                //         // x.getReading().getRank();
                //         // });

                //         List<ComplexReadingModel> hTargetSortedHypernodeReadings = hTargetReadings
                //                 .stream()
                //                 .sorted(
                //                         java.util.Comparator.comparing(x -> x.getReading().getRank()

                //                         )).collect(Collectors.toList());

                //         // List<ComplexReadingModel> sortedHypernodeReadings =
                //         // hypernodeReadings.stream()
                //         // .sorted(Comparator.comparing(x -> x.getReading().getRank()))
                //         // .collect(Collectors.toList());

                //         for (ComplexReadingModel htrm : hTargetSortedHypernodeReadings) {

                //             ReadingModel reading = htrm.getReading();
                //             writer.writeCharacters("text:" + reading.getText());
                //             writer.writeCharacters(" | getID:" + reading.getId());
                //             writer.writeCharacters(" | getRank:" + reading.getRank().toString());
                //             writer.writeCharacters(" | getWitnesses:" + reading.getWitnesses().toString());
                //         }

                //         writer.writeEndElement(); // rdg

                //         // ANNOTATION FOR RELATION
                //         if (relationship.getAnnotation() != null && !relationship.getAnnotation().isEmpty()) {
                //             writer.writeStartElement("note");
                //             writer.writeCharacters(relationship.getAnnotation());
                //             writer.writeEndElement(); // note
                //         }

                //         writer.writeEndElement(); // app

                //     }

                //     if (!relationship.getIs_hyperrelation()) {
                //         // GET STANDARD RELATION SOURCE NODE
                //         Node source = db.getNodeById(Long.parseLong(relationship.getSource()));
                //         ReadingModel sourceRM = new ReadingModel(source);

                //         // OPEN CRITICAL APPARATUS
                //         writer.writeStartElement("rdg"); // OPEN LEMMA/RDG
                //         writer.writeAttribute("xml:id", sourceRM.getId());

                //         // getWitnessList of rdg
                //         List<String> lem_wits = sourceRM.getWitnesses();
                //         ArrayList<String> sigList = new ArrayList<>();
                //         for (String l : lem_wits)
                //             sigList.add("#" + l);
                //         Collections.sort(sigList);
                //         String witnessList = String.join(" ", sigList);
                //         writer.writeAttribute("wit", witnessList);

                //         // getRank
                //         writer.writeAttribute("rank", Long.toString(sourceRM.getRank()));
                //         writer.writeCharacters(sourceRM.getText()); // write inner text

                //         writer.writeEndElement(); // rdg

                //         // GET STANDARD RELATION SOURCE NODE
                //         Node target = db.getNodeById(Long.parseLong(relationship.getTarget()));
                //         ReadingModel targetRM = new ReadingModel(target);

                //         // OPEN CRITICAL APPARATUS
                //         writer.writeStartElement("rdg"); // OPEN LEMMA/RDG
                //         writer.writeAttribute("xml:id", targetRM.getId());

                //         // getWitnessList of rdg
                //         List<String> target_wits = targetRM.getWitnesses();
                //         sigList = new ArrayList<>();
                //         for (String l : target_wits)
                //             sigList.add("#" + l);
                //         Collections.sort(sigList);
                //         witnessList = String.join(" ", sigList);
                //         writer.writeAttribute("wit", witnessList);

                //         // getRank
                //         writer.writeAttribute("rank", Long.toString(targetRM.getRank()));
                //         writer.writeCharacters(targetRM.getText()); // write inner text

                //         writer.writeEndElement(); // rdg
                //         writer.writeEndElement(); // app

                //     }
                // }

                // writer.writeEndElement(); // relationshiplist

                writer.writeStartElement("div");
                writer.writeStartElement("p");

                VariantListModel vlm = new VariantListModel(
                        sectionNode, baseWitness, excWitnesses, conflate, suppressMatching,
                        !excludeNonsense.equals("no"), !excludeType1.equals("no"),
                        significant, !combine.equals("no"));

                for (ReadingModel readingModel : vlm.getBaseChain()) {
                    List<VariantLocationModel> variantLocationFound = vlm.getVariantlist().stream()
                            .filter(y -> y.getRankIndex().equals(readingModel.getRank()))
                            .collect(Collectors.toList());

                    String readingId = readingModel.getId();
                    Boolean defined_by_manual_relation = false;
                    Boolean defined_by_hyperrelation = false;
                    for (RelationStruct relStruct : relStructList) {



                        if (relStruct.relationModel.getIs_hyperrelation()
                                && relStruct.relationModel.getTarget().equals(readingId)) 
                            {
                            // writer.writeEmptyElement("hyper_relationship");
                            defined_by_hyperrelation = true;

                            // writer.writeStartElement("debug_relationship");


                            // /// DEBUG PURPOSES
                            // writer.writeCharacters(Boolean.toString(relStruct.relationModel.getIs_hyperrelation()));
                            // writer.writeCharacters(readingId);
                            // writer.writeCharacters(" | source  : " + relStruct.relationModel.getSource());
                            // writer.writeCharacters(" | hsource : " + relStruct.relationModel.getHSource());
                            // writer.writeCharacters(" | target : " + relStruct.relationModel.getTarget());
                            // writer.writeCharacters(" | htarget : " + relStruct.relationModel.getHTarget());
                            // writer.writeCharacters(" | type : " + relStruct.relationModel.getType());
                            // writer.writeEndElement(); // app

                            // OPEN group of rdgGrp
                            // writer.writeStartElement("rdgGrp");

                            // Critical Apparatus of an HYPER-RELATION
                            writer.writeStartElement("app");
                            writer.writeAttribute("cause", relStruct.relationModel.getType());
                            writer.writeAttribute("xml:id", relStruct.relationModel.getId());

                            writer.writeStartElement("annotation");
                            writer.writeAttribute("motivation", "assesing");
                            writer.writeAttribute("target", relStruct.relationModel.getId());
                            writer.writeCharacters(relStruct.relationModel.getAnnotation());
                            writer.writeEndElement(); // annotation

                            // Rdg of the SOURCE HYPERNODE
                            writer.writeStartElement("rdgGrp");
                            writer.writeAttribute("xml:id", relStruct.hSource.getId());

                            List<String> sigList = new ArrayList<>();
                            for (ReadingModel rm : relStruct.hTargetReadings) {
                                // for each rm get the witnesses and add them to the attribute wit 
                                // avoiding duplicates
                                List<String> wits = rm.getWitnesses();
                                for (String l : wits)
                                    sigList.add("#" + l);
                            }
                            sigList = sigList.stream().distinct().collect(Collectors.toList());
                            Collections.sort(sigList);
                            String witnessList = String.join(" ", sigList);
                            writer.writeAttribute("wit", witnessList);
                            
                            for (ReadingModel rm : relStruct.hSourceReadings) {
                                //print the rdg of each reading model
                                writer.writeStartElement("rdg");
                                writer.writeAttribute("xml:id", rm.getId());
                                List<String> wits = rm.getWitnesses();
                                sigList = new ArrayList<>();
                                for (String l : wits)
                                    sigList.add("#" + l);
                                Collections.sort(sigList);
                                witnessList = String.join(" ", sigList);
                                writer.writeAttribute("wit", witnessList);
                                writer.writeAttribute("rank", Long.toString(rm.getRank()));
                                writer.writeCharacters(rm.getText());
                                writer.writeEndElement(); // rdg
                            }
                            writer.writeEndElement();

                            writer.writeStartElement("rdg");
                            writer.writeAttribute("xml:id", relStruct.hTarget.getId());

                            
                            sigList = new ArrayList<>();
                            for (ReadingModel rm : relStruct.hTargetReadings) {
                                List<String> wits = rm.getWitnesses();
                                for (String l : wits)
                                    sigList.add("#" + l);
                            }
                            sigList = sigList.stream().distinct().collect(Collectors.toList());
                            Collections.sort(sigList);
                            witnessList = String.join(" ", sigList);
                            writer.writeAttribute("wit", witnessList);
                            writer.writeAttribute("rank", Long.toString(relStruct.targetNode.getRank()));


                            for (ReadingModel rm : relStruct.hTargetReadings) {
                                 //print the rdg of each reading model
                                 writer.writeStartElement("rdg");
                                 writer.writeAttribute("xml:id", rm.getId());
                                 List<String> wits = rm.getWitnesses();
                                 sigList = new ArrayList<>();
                                 for (String l : wits)
                                     sigList.add("#" + l);
                                 Collections.sort(sigList);
                                 witnessList = String.join(" ", sigList);
                                 writer.writeAttribute("wit", witnessList);
                                 writer.writeAttribute("rank", Long.toString(rm.getRank()));
                                 writer.writeCharacters(rm.getText());
                                 writer.writeEndElement(); // rdg
                            }
                            writer.writeEndElement(); // hypernode target readings

                            writer.writeEndElement(); // app

                            // writer.writeEndElement(); // rdgGrp
                        }

                        // if is not a hyperrelation it means it is a standard relation
                        // raised by an editors hand and links two individual readings
                        // as relationship between individual standard readings can only be done
                        // vertically and not horizontally, we can print the relationship
                        // as a standard apparatus with the cause attribute set on the target reading

                        if (!relStruct.relationModel.getIs_hyperrelation()
                                && relStruct.relationModel.getTarget().equals(readingId)) {
                            writer.writeStartElement("app"); // app
                            
                            // writer.writeEmptyElement("normal_relationship");
                            defined_by_manual_relation = true;

                            writer.writeAttribute("xml:id", relStruct.relationModel.getId());
                            writer.writeAttribute("cause", relStruct.relationModel.getType());

                            writer.writeStartElement("annotation");
                            writer.writeAttribute("motivation", "assesing");
                            writer.writeAttribute("target", relStruct.relationModel.getId());
                            writer.writeCharacters(relStruct.relationModel.getAnnotation());
                            writer.writeEndElement(); // annotation

                            
                            // writer.writeStartElement("debug_relationship");

                            // /// DEBUG PURPOSES
                            // writer.writeCharacters(Boolean.toString(relStruct.relationModel.getIs_hyperrelation()));
                            // writer.writeCharacters(readingId);
                            // writer.writeCharacters(" | source  : " + relStruct.relationModel.getSource());
                            // writer.writeCharacters(" | hsource : " + relStruct.relationModel.getHSource());
                            // writer.writeCharacters(" | target : " + relStruct.relationModel.getTarget());
                            // writer.writeCharacters(" | htarget : " + relStruct.relationModel.getHTarget());
                            // writer.writeCharacters(" | type : " + relStruct.relationModel.getType());
                            // writer.writeEndElement(); // app

                            writer.writeStartElement("rdg");
                            writer.writeAttribute("xml:id", relStruct.sourceNode.getId());
                            List<String> wits = relStruct.sourceNode.getWitnesses();
                            ArrayList<String> sigList = new ArrayList<>();
                            for (String l : wits)
                                sigList.add("#" + l);
                            Collections.sort(sigList);
                            String witnessList = String.join(" ", sigList);
                            writer.writeAttribute("wit", witnessList);
                            writer.writeAttribute("rank", Long.toString(relStruct.sourceNode.getRank()));
                            writer.writeCharacters(relStruct.sourceNode.getText());
                            writer.writeEndElement(); // rdg

                            writer.writeStartElement("rdg");
                            writer.writeAttribute("xml:id", relStruct.targetNode.getId());
                            wits = relStruct.targetNode.getWitnesses();
                            sigList = new ArrayList<>();
                            for (String l : wits)
                                sigList.add("#" + l);
                            Collections.sort(sigList);
                            witnessList = String.join(" ", sigList);
                            writer.writeAttribute("wit", witnessList);
                            writer.writeAttribute("rank", Long.toString(relStruct.targetNode.getRank()));
                            writer.writeCharacters(relStruct.targetNode.getText());
                            writer.writeEndElement(); // rdg

                            writer.writeEndElement(); // app

                        }

                    }


                    // If the reading has variants, 
                    // ans is not a standard manual relationship
                    // build up the critical apparatus.
                    
                    if (!variantLocationFound.isEmpty() && !defined_by_manual_relation) {
                        try {
                            // OPEN CRITICAL APPARATUS
                            writer.writeStartElement("app");
                            writer.writeStartElement("rdg"); // OPEN LEMMA
                            writer.writeAttribute("xml:id", readingModel.getId());

                            // getWitnessList of lemma
                            List<String> lem_wits = readingModel.getWitnesses();
                            ArrayList<String> sigList = new ArrayList<>();
                            for (String l : lem_wits)
                                sigList.add("#" + l);
                            Collections.sort(sigList);
                            String witnessList = String.join(" ", sigList);
                            writer.writeAttribute("wit", witnessList);

                            // getRank
                            writer.writeAttribute("rank", Long.toString(readingModel.getRank()));

                            writer.writeCharacters(readingModel.getText()); // write inner text

                            writer.writeEndElement(); // lem

                            VariantLocationModel vloc = variantLocationFound.get(0);
                            for (VariantModel vm : vloc.getVariants()) {

                                writer.writeStartElement("rdg");

                                if (vm.getReadings().size() > 0) {
                                    writer.writeAttribute("xml:id", vm.getReadings().get(0).getId());
                                }

                                // getWitnessList
                                Map<String, List<String>> wits = vm.getWitnesses();
                                ArrayList<String> variantSigList = new ArrayList<>();
                                for (String l : wits.keySet())
                                    for (String s : wits.get(l))
                                        variantSigList.add(
                                                l.equals("witnesses") ? "#" + s
                                                        : "#" + String.format("%s (%s)", s, l));
                                Collections.sort(variantSigList);
                                String variantWitnessList = String.join(" ", variantSigList);
                                writer.writeAttribute("wit", variantWitnessList);

                                // getRanksList
                                // Empty nodes have no rank and make the export fail
                                if (vm.getReadings().size() > 0) {

                                    String variantRank = Long.toString(vm.getReadings().get(0).getRank());
                                    writer.writeAttribute("rank", variantRank);
                                }

                                String varText = ReadingService.textOfReadings(vm.getReadings(),
                                        vloc.isNormalised(),
                                        false);
                                writer.writeCharacters(varText);
                                writer.writeEndElement(); // rdg
                            }
                            writer.writeEndElement(); // app
                            writer.writeCharacters(" ");
                            
                        } catch (XMLStreamException e) {

                            writer.writeEmptyElement(e.getMessage());

                            // e.printStackTrace();
                        }


                    }
                    // If the reading has no variants, just write it out
                    else {
                        try {
                            if(!defined_by_hyperrelation && !defined_by_manual_relation) {
                                writer.writeCharacters(readingModel.getText() + " ");
                            }
                            
                            

                        } catch (XMLStreamException e) {
                            writer.writeEmptyElement(e.getMessage());

                            // e.printStackTrace();
                        }
                    }


                }

                writer.writeEndElement(); // p
                writer.writeEndElement(); // div

            }

            writer.writeEndElement(); // body
            writer.writeEndElement(); // text
            writer.writeEndElement(); // TEI

            writer.flush();

            return Response.ok(
                new String(result.toString().getBytes(), StandardCharsets.UTF_16)
                
                , MediaType.APPLICATION_XML).build();
            // MediaType.APPLICATION_JSON_TYPE).build()
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }

        
    }


    private static class TabularExporterException extends Exception {
        TabularExporterException(String message) {
            super(message);
        }
    }

    private class RelationStruct {

        public RelationModel relationModel;
        // HYPERNODES OBJECTS RECOVERY (used if is_hyperrelation is true)
        public ComplexReadingModel hSource;
        public ComplexReadingModel hTarget;
        public List<ReadingModel> hSourceReadings;
        public List<ReadingModel> hTargetReadings;

        // STANDARD NODES (used if is_hyperrelation is false)
        public ReadingModel sourceNode;
        public ReadingModel targetNode;

        public RelationStruct() {
        }

    }

}
