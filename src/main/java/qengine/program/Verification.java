package qengine.program;

import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.query.api.Query;
import fr.boreal.model.queryEvaluation.api.FOQueryEvaluator;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;
import qengine.storage.RDFHexaStore;

import java.io.*;
import java.util.*;

public final class Verification {

    private static final String WORKING_DIR = "data/";
    private static final String SAMPLE_DATA_FILE = WORKING_DIR + "100K.nt";
    private static final String SAMPLE_QUERY_FILE = WORKING_DIR + "STAR_ALL_workload.queryset";
    private static final String OUTPUT_DIR = "results/";

    public static void main(String[] args) throws IOException {
        // Création du dossier de résultats
        new File(OUTPUT_DIR).mkdirs();

        System.out.println("=== Parsing RDF Data ===");
        List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE);

        System.out.println("\n=== Parsing Sample Queries ===");
        List<StarQuery> starQueries = parseSPARQLQueries(SAMPLE_QUERY_FILE);

        // Initialisation des moteurs
        RDFHexaStore myStore = new RDFHexaStore();
        FactBase integraalStore = new SimpleInMemoryGraphStore();

        // Charger les données RDF dans les deux moteurs
        rdfAtoms.forEach(atom -> {
            myStore.add(atom);
            integraalStore.add(atom);
        });

        System.out.println("\n=== Comparing Results Between Systems ===");
        compareSystems(myStore, integraalStore, starQueries, OUTPUT_DIR);
    }

    /**
     * Parse un fichier RDF et retourne une liste de RDFAtoms.
     */
    private static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
        List<RDFAtom> rdfAtoms = new ArrayList<>();
        try (RDFAtomParser rdfParser = new RDFAtomParser(new FileReader(rdfFilePath), RDFFormat.NTRIPLES)) {
            while (rdfParser.hasNext()) {
                rdfAtoms.add(rdfParser.next());
            }
        }
        System.out.println("Total RDF Atoms parsed: " + rdfAtoms.size());
        return rdfAtoms;
    }

    /**
     * Parse un fichier SPARQL et retourne une liste de StarQueries.
     */
    private static List<StarQuery> parseSPARQLQueries(String queryFilePath) throws IOException {
        List<StarQuery> starQueries = new ArrayList<>();
        try (StarQuerySparQLParser queryParser = new StarQuerySparQLParser(queryFilePath)) {
            while (queryParser.hasNext()) {
                Query query = queryParser.next();
                if (query instanceof StarQuery) {
                    starQueries.add((StarQuery) query);
                }
            }
        }
        System.out.println("Total Queries parsed: " + starQueries.size());
        return starQueries;
    }

    /**
     * Compare les résultats entre le système et Integraal.
     */
    private static void compareSystems(RDFHexaStore myStore, FactBase integraalStore, List<StarQuery> starQueries, String outputDir) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputDir + "comparison_results.txt"))) {
            GenericFOQueryEvaluator evaluator = GenericFOQueryEvaluator.defaultInstance();

            for (StarQuery query : starQueries) {
                // Exécution de la requête sur notre système
                List<Substitution> myResults = new ArrayList<>();
                myStore.match(query).forEachRemaining(myResults::add);

                // Exécution de la requête sur Integraal
                FOQuery<FOFormulaConjunction> integraalQuery = query.asFOQuery();
                Iterator<Substitution> integraalIterator = evaluator.evaluate(integraalQuery, integraalStore);
                List<Substitution> integraalResults = new ArrayList<>();
                integraalIterator.forEachRemaining(integraalResults::add);

                // Comparaison des résultats
                Set<Substitution> missingResults = new HashSet<>(integraalResults);
                missingResults.removeAll(myResults);

                Set<Substitution> extraResults = new HashSet<>(myResults);
                extraResults.removeAll(integraalResults);

                // Rapport des résultats
                writer.println("Query: " + query.getLabel());
                writer.println("My Results: " + myResults);
                writer.println("Integraal Results: " + integraalResults);
                if (missingResults.isEmpty() && extraResults.isEmpty()) {
                    writer.println("Status: Correct and Complete");
                } else {
                    writer.println("Missing Results: " + missingResults);
                    writer.println("Extra Results: " + extraResults);
                    writer.println("Status: Issues Detected");
                }
                writer.println();
            }
        }
    }
}