package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.util.*;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {


    private final HashMap<Term,Integer> dictionnaire ;
    private final HashMap<Integer,Term> decodeur ;
    private final AtomicInteger compteur ;
    private final Map<Integer,Map<Integer,List<Integer>>> spo ;
    private final Map<Integer,Map<Integer,List<Integer>>> sop ;
    private final Map<Integer,Map<Integer,List<Integer>>> pso ;
    private final Map<Integer,Map<Integer,List<Integer>>> pos ;
    private final Map<Integer,Map<Integer,List<Integer>>> osp ;
    private final Map<Integer,Map<Integer,List<Integer>>> ops ;

    public RDFHexaStore() {
        this.dictionnaire = new HashMap<>();
        this.decodeur = new HashMap<>();
        this.compteur = new AtomicInteger(0);
        this.spo = new HashMap<>();
        this.sop = new HashMap<>();
        this.pso = new HashMap<>();
        this.pos = new HashMap<>();
        this.osp = new HashMap<>();
        this.ops = new HashMap<>();
    }


    @Override
    public boolean add(RDFAtom atom) {
        Term s = atom.getTripleSubject();
        Term p = atom.getTriplePredicate();
        Term o = atom.getTripleObject();
        Integer idxS = dictionnaire.computeIfAbsent(s, k -> compteur.getAndIncrement());
        Integer idxP = dictionnaire.computeIfAbsent(p, k -> compteur.getAndIncrement());
        Integer idxO = dictionnaire.computeIfAbsent(o, k -> compteur.getAndIncrement());

        decodeur.put(idxS, s);
        decodeur.put(idxP, p);
        decodeur.put(idxO, o);

        spo.computeIfAbsent(idxS, k -> new HashMap<>())
                .computeIfAbsent(idxP, k -> new ArrayList<>())
                .add(idxO);
        sop.computeIfAbsent(idxS, k -> new HashMap<>())
                .computeIfAbsent(idxO, k -> new ArrayList<>())
                .add(idxP);
        pso.computeIfAbsent(idxP, k -> new HashMap<>())
                .computeIfAbsent(idxS, k -> new ArrayList<>())
                .add(idxO);
        pos.computeIfAbsent(idxP, k -> new HashMap<>())
                .computeIfAbsent(idxO, k -> new ArrayList<>())
                .add(idxS);
        osp.computeIfAbsent(idxO, k -> new HashMap<>())
                .computeIfAbsent(idxS, k -> new ArrayList<>())
                .add(idxP);
        ops.computeIfAbsent(idxO, k -> new HashMap<>())
                .computeIfAbsent(idxP, k -> new ArrayList<>())
                .add(idxS);

        return true;
    }
    public void getIndexes() {
        // print indexes
        System.out.println("spo : " + spo);
        System.out.println("sop : " + sop);
        System.out.println("pso : " + pso);
        System.out.println("pos : " + pos);
        System.out.println("osp : " + osp);
        System.out.println("ops : " + ops);

    }
    @Override
    public long size() {
        throw new NotImplementedException();

    }

public Iterator<Substitution> match(RDFAtom atom) {
    Term subject = atom.getTripleSubject();
    Term predicate = atom.getTriplePredicate();
    Term object = atom.getTripleObject();

    List<Integer> matches = null;

    if (isVariable(subject)) {
        // Subject is a variable: match based on predicate and object
        matches = getMatches(pos, predicate, object);
    } else if (isVariable(predicate)) {
        // Predicate is a variable: match based on subject and object
        matches = getMatches(osp, object, subject);
    } else if (isVariable(object)) {
        // Object is a variable: match based on subject and predicate
        matches = getMatches(spo, subject, predicate);
    }

    // If no matches, return empty iterator
    if (matches == null || matches.isEmpty()) {
        return Collections.emptyIterator();
    }

    // Generate substitutions from matches
    return createSubstitutions(matches, subject, predicate, object).iterator();
}


    private List<Integer> getMatches(
            Map<Integer, Map<Integer, List<Integer>>> index,
            Term key1,
            Term key2
    ) {
        if (isVariable(key1) || isVariable(key2)) {
            return Collections.emptyList();
        }

        Integer idx1 = dictionnaire.get(key1);
        if (idx1 == null || !index.containsKey(idx1)) {
            return Collections.emptyList();
        }

        Map<Integer, List<Integer>> subMap = index.get(idx1);
        if (key2 != null) {
            Integer idx2 = dictionnaire.get(key2);
            if (idx2 != null && subMap.containsKey(idx2)) {
                return subMap.get(idx2);
            } else {
                return Collections.emptyList();
            }
        } else {
            // Collect all matches for the first term
            return subMap.values().stream().flatMap(List::stream).collect(Collectors.toList());
        }
    }

    private boolean isVariable(Term term) {
        return term instanceof Variable;
    }

    private List<Substitution> createSubstitutions(
            List<Integer> matches,
            Term subject,
            Term predicate,
            Term object
    ) {
        List<Substitution> substitutions = new ArrayList<>();
        for (Integer match : matches) {
            Substitution sub = new SubstitutionImpl();
            if (isVariable(subject)) {
                sub.add((Variable) subject, decodeur.get(match));
            }
            if (isVariable(predicate)) {
                sub.add((Variable) predicate, decodeur.get(match));
            }
            if (isVariable(object)) {
                sub.add((Variable) object, decodeur.get(match));
            }
            substitutions.add(sub);
        }
        return substitutions;
    }



    @Override
    public Iterator<Substitution> match(StarQuery q) {
        var atoms = q.getRdfAtoms();
        if (atoms == null || atoms.isEmpty()) {
            return Collections.emptyIterator(); // No atoms to match
        }

        Iterator<Substitution> firstMatches = this.match(atoms.get(0));
        List<Substitution> intersection = new ArrayList<>();
        firstMatches.forEachRemaining(intersection::add);

        for (int i = 1; i < atoms.size(); i++) {
            Iterator<Substitution> currentMatches = this.match(atoms.get(i));
            List<Substitution> currentList = new ArrayList<>();
            currentMatches.forEachRemaining(currentList::add);
            intersection.retainAll(currentList);
            if (intersection.isEmpty()) {
                return Collections.emptyIterator();
            }
        }
        return intersection.iterator();
    }

    @Override
    public Collection<Atom> getAtoms() {
        throw new NotImplementedException();
    }
}
