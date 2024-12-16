package qengine.program;

import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.logicalElements.api.Literal;
import fr.boreal.model.logicalElements.api.Variable;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.query.api.Query;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.logicalElements.api.Substitution;
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
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;

public final class Example {

	private static final String WORKING_DIR = "data/";
	private static final String SAMPLE_DATA_FILE = WORKING_DIR + "sample_data.nt";
	private static final String SAMPLE_QUERY_FILE = WORKING_DIR + "sample_query.queryset";

	private static final String H_DATA_FILE = WORKING_DIR + "100k.nt";
	private static final String QUERY_FILE = WORKING_DIR + "STAR_ALL_workload.queryset";
	public static void main(String[] args) throws IOException {
		/*
		 * Exemple d'utilisation des deux parsers
		 */
		System.out.println("=== Parsing RDF Data ===");
		List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE);

		System.out.println("\n=== Parsing Sample Queries ===");
		List<StarQuery> starQueries = parseSparQLQueries(SAMPLE_QUERY_FILE);

		/*
		 * Exemple d'utilisation de l'évaluation de requetes par Integraal avec les objets parsés
		 */
		System.out.println("\n=== Executing the queries with Integraal ===");
		FactBase factBase = new SimpleInMemoryGraphStore();

		for (RDFAtom atom : rdfAtoms) {
			factBase.add(atom);  // Stocker chaque RDFAtom dans le store
		}
		RDFHexaStore rdfHexaStore = new RDFHexaStore();
		rdfAtoms.forEach(rdfHexaStore::add);

//		RDFHexaStore rdf = new RDFHexaStore();
//		rdf.addAll(rdfAtoms.stream());

		final Variable VAR_X = SameObjectTermFactory.instance().createOrGetVariable("?x");
		final Variable VAR_Y = SameObjectTermFactory.instance().createOrGetVariable("?y");
		final Variable VAR_Z = SameObjectTermFactory.instance().createOrGetVariable("?z");
		final Literal<String> SUBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("http://db.uwaterloo.ca/~galuc/wsdbm/User1");
		final Literal<String> OBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("2536508");

		RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, VAR_Y, OBJECT_1);


		System.out.println("########################");
		rdfHexaStore.match(matchingAtom).forEachRemaining(System.out::println);
		System.out.println("########################");
		// Exécuter les requêtes sur le store
		int i = 0;
		for (StarQuery starQuery : starQueries) {
//			var one = executeStarQuery(starQuery, factBase);
//			var two = executeStarQuery2(starQuery, rdfHexaStore);
////			saveResults(starQuery, one, two, i+".txt");
			i++;
		}
	}

	/**
	 * Parse et affiche le contenu d'un fichier RDF.
	 *
	 * @param rdfFilePath Chemin vers le fichier RDF à parser
	 * @return Liste des RDFAtoms parsés
	 */
	private static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
		FileReader rdfFile = new FileReader(rdfFilePath);
		List<RDFAtom> rdfAtoms = new ArrayList<>();

		try (RDFAtomParser rdfAtomParser = new RDFAtomParser(rdfFile, RDFFormat.NTRIPLES)) {
			int count = 0;
			while (rdfAtomParser.hasNext()) {
				RDFAtom atom = rdfAtomParser.next();
				rdfAtoms.add(atom);  // Stocker l'atome dans la collection
				System.out.println("RDF Atom #" + (++count) + ": " + atom);
			}
			System.out.println("Total RDF Atoms parsed: " + count);
		}
		return rdfAtoms;
	}

	/**
	 * Parse et affiche le contenu d'un fichier de requêtes SparQL.
	 *
	 * @param queryFilePath Chemin vers le fichier de requêtes SparQL
	 * @return Liste des StarQueries parsées
	 */
	private static List<StarQuery> parseSparQLQueries(String queryFilePath) throws IOException {
		List<StarQuery> starQueries = new ArrayList<>();

		try (StarQuerySparQLParser queryParser = new StarQuerySparQLParser(queryFilePath)) {
			int queryCount = 0;

			while (queryParser.hasNext()) {
				Query query = queryParser.next();
				if (query instanceof StarQuery starQuery) {
					starQueries.add(starQuery);  // Stocker la requête dans la collection
					System.out.println("Star Query #" + (++queryCount) + ":");
					System.out.println("  Central Variable: " + starQuery.getCentralVariable().label());
					System.out.println("  RDF Atoms:");
					starQuery.getRdfAtoms().forEach(atom -> System.out.println("    " + atom));
				} else {
					System.err.println("Requête inconnue ignorée.");
				}
			}
			System.out.println("Total Queries parsed: " + starQueries.size());
		}
		return starQueries;
	}

	/**
	 * Exécute une requête en étoile sur le store et affiche les résultats.
	 *
	 * @param starQuery La requête à exécuter
	 * @param factBase  Le store contenant les atomes
	 */
	private static Iterator<Substitution> executeStarQuery(StarQuery starQuery, FactBase factBase) {
		FOQuery<FOFormulaConjunction> foQuery = starQuery.asFOQuery(); // Conversion en FOQuery
		FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance(); // Créer un évaluateur
		Iterator<Substitution> queryResults = evaluator.evaluate(foQuery, factBase); // Évaluer la requête

		System.out.printf("Execution of  %s:%n", starQuery);
		System.out.println("Answers:");
		if (!queryResults.hasNext()) {
			System.out.println("No answer.");
		}
		while (queryResults.hasNext()) {
			Substitution result = queryResults.next();
			System.out.println(result); // Afficher chaque réponse
		}
		System.out.println();
		return queryResults;
	}
	private static Iterator<Substitution> executeStarQuery2(StarQuery starQuery, RDFHexaStore rdfHexaStore) {
			var result = rdfHexaStore.match(starQuery);
			System.out.printf("Execution of  %s:%n MINE \n", starQuery);
			System.out.println("Answers:");
			if (!result.hasNext()) {
				System.out.println("No answer.");
			}
			while (result.hasNext()) {
				Substitution substitution = result.next();
				System.out.println(substitution); // Afficher chaque réponse
			}
			System.out.println();
			return result;
	}
	private static void saveResults(StarQuery starQuery, Iterator<Substitution> queryResults1,Iterator<Substitution> queryResults2,String filename) {

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
			writer.write("Execution of  " + starQuery + ":\n");
			writer.write("Answers:\n");
			writer.write("MINE\n");
			if (!queryResults1.hasNext()) {
				writer.write("No answer.\n");
			}
			while (queryResults1.hasNext()) {
				Substitution substitution = queryResults1.next();
				writer.write(substitution + "\n");
			}
			writer.write("INTEGRAAL\n");
			if (!queryResults2.hasNext()) {
				writer.write("No answer.\n");
			}
			while (queryResults2.hasNext()) {
				Substitution substitution = queryResults2.next();
				writer.write(substitution + "\n");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
