package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests unitaires pour la classe {@link RDFHexaStore}.
 */
public class RDFHexaStoreTest {
    private static final Literal<String> SUBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("subject1");
    private static final Literal<String> PREDICATE_1 = SameObjectTermFactory.instance().createOrGetLiteral("predicate1");
    private static final Literal<String> OBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("object1");
    private static final Literal<String> SUBJECT_2 = SameObjectTermFactory.instance().createOrGetLiteral("subject2");
    private static final Literal<String> PREDICATE_2 = SameObjectTermFactory.instance().createOrGetLiteral("predicate2");
    private static final Literal<String> OBJECT_2 = SameObjectTermFactory.instance().createOrGetLiteral("object2");
    private static final Literal<String> OBJECT_3 = SameObjectTermFactory.instance().createOrGetLiteral("object3");
    private static final Variable VAR_X = SameObjectTermFactory.instance().createOrGetVariable("?x");
    private static final Variable VAR_Y = SameObjectTermFactory.instance().createOrGetVariable("?y");


    @Test
    public void testAddAllRDFAtoms() {
        RDFHexaStore store = new RDFHexaStore();

        // Version stream
        // Ajouter plusieurs RDFAtom
        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);

        Set<RDFAtom> rdfAtoms = Set.of(rdfAtom1, rdfAtom2);

        assertTrue(store.addAll(rdfAtoms.stream()), "Les RDFAtoms devraient être ajoutés avec succès.");

        // Vérifier que tous les atomes sont présents
        Collection<Atom> atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom1), "La base devrait contenir le premier RDFAtom ajouté.");
        assertTrue(atoms.contains(rdfAtom2), "La base devrait contenir le second RDFAtom ajouté.");

        // Version collection
        store = new RDFHexaStore();
        assertTrue(store.addAll(rdfAtoms), "Les RDFAtoms devraient être ajoutés avec succès.");

        // Vérifier que tous les atomes sont présents
        atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom1), "La base devrait contenir le premier RDFAtom ajouté.");
        assertTrue(atoms.contains(rdfAtom2), "La base devrait contenir le second RDFAtom ajouté.");
    }

    @Test
    public void testAddRDFAtom() {
        throw new NotImplementedException();
    }

    @Test
    public void testAddDuplicateAtom() {
        throw new NotImplementedException();
    }

    @Test
    public void testSize() {
        throw new NotImplementedException();
    }

    @Test
    public void testMatchAtom() {
        RDFHexaStore store = new RDFHexaStore();
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1)); // RDFAtom(subject1, triple, object1)
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_2)); // RDFAtom(subject2, triple, object2)
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_3)); // RDFAtom(subject1, triple, object3)

        // Case 1
        RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, VAR_X); // RDFAtom(subject1, predicate1, X)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, OBJECT_1);
        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, OBJECT_3);

        assertEquals(2, matchedList.size(), "There should be two matched RDFAtoms");
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + firstResult);
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + secondResult);

        // Other cases
        throw new NotImplementedException("This test must be completed");
    }

    @Test
    public void testMatchStarQuery() {
        throw new NotImplementedException();
    }

    // Vos autres tests d'HexaStore ici
}
