# Mini-Projet RDF Partie 2 : Évaluation et Analyse des Performances

## Objectif

L'objectif de cette deuxième partie du projet est d'évaluer et analyser les performances de votre prototype.  
Précisément, les performances de votre système doivent être comparées en utilisant le benchmark WatDiv avec :

1. La dernière version de InteGraal.
2. Une implémentation réalisée par vos collègues (à vous de choisir laquelle).  

## Travail à rendre

1. Analyse des bancs d'essai et plan des tests à réaliser. Pas de rendu dans Moodle, mais le plan de test sera discuté en TP le **6 décembre**.
2. Réalisation des tests et document final : **~15~22 décembre** (rendu dans Moodle).

---
**Les réponses aux questions suivantes doivent être incluses dans le rapport final du projet, que vous pouvez organiser librement. La seule contrainte concerne la taille du rapport qui ne doit pas dépasser les 10 pages (annexes exclus).**


---

## Benchmarks

1. Quels **types** de benchmark avez-vous utilisés pour vos tests pendant le développement ?  
2. Décrivez un test possible pour chaque **type** de benchmark (micro, standard, réel).
3. Vous allez utiliser le benchmark [WatDiv](http://dsg.uwaterloo.ca/watdiv/) pour vos tests.  
   Présentez brièvement le benchmark, qu'est WatDiv ?

---

## Préparation des bancs d'essais

Attention   : 
- ⚠️ Le fonctionnement de Watdiv n'est garanti que dans les ordinateurs de la faculté.
- ⚠️ Utilisez la version de Watdiv preconfigurée qui est mise à disposition dans ce dépôt.


1. Utiliser la version de WatDiv mise à disposition pour générer des bases de données de tailles différentes, ainsi que des jeux de requêtes pour vos tests.  
   - **Pour générer des données** : Compilez WatDiv (C++) selon les instructions ci-dessous ou sur le [site de WatDiv](https://dsg.uwaterloo.ca/watdiv/#installation). Vérifiez que la bibliothèque **BOOST** est installée.
   - **Format des données** : WatDiv génère des fichiers au format N3. Si nécessaire, utilisez [rdf2rdf](http://www.l3s.de/~minack/rdf2rdf/) pour les convertir en RDF/XML.
   - **Pour générer des requêtes** : Modifiez le script `regenerate_queryset.sh` pour ajuster le nombre de requêtes par template (par défaut : 100).  
     Des patrons de requêtes sont disponibles dans :
     - Le répertoire `testsuite` pour les requêtes **en étoile**.
     - Le répertoire `more query templates` pour des requêtes générales. Vous pouvez consulter leur description [ici](https://dsg.uwaterloo.ca/watdiv/basic-testing.shtml).

2. Créer un jeu de tests pour :
   - Les requêtes **en étoile**.

   ⚠️ La validité des expériences dépend de ce passage clé. Vous devrez générer plusieurs requêtes avec WatDiv.

3. Une fois votre jeu de requêtes de test créé, représentez les résultats avec un histogramme montrant :
   - Le nombre de réponses aux requêtes sur une instance de **500K** et de **2M** triples.
4. Combien de requêtes ont zéro réponses ?  
5. Combien de requêtes sont en étoile ?

---

## Hardware et Software

1. Quel type de **hardware** et de **software** avez-vous utilisé pour vos tests précédents ?  
2. Est-ce adapté à l’analyse des performances du système ? Justifiez.  

---

## Métriques, Facteurs, et Niveaux

1. Donnez une liste de **métriques** permettant d’évaluer les moteurs de requêtes RDF.  
2. Listez les **facteurs** qui interviennent dans l'évaluation du système, et définissez-en les niveaux.  
3. Ordonnez les facteurs par importance et identifiez les facteurs principaux et secondaires.

---

## Évaluation des performances

1. Pour quelles métriques est-il préférable d’effectuer des mesures **“cold”** ou **“warm”** (ou les deux) ?  
2. Comment allez-vous réaliser ces mesures en pratique ?  
3. Proposez une procédure pour vérifier la correction et la complétude de votre système.  
   - Tester les systèmes sur des requêtes en étoile et, si votre système le permet, sur des requêtes générales.
4. Réalisez une expérience **2²** en faisant varier la taille des données et la mémoire. Que pouvez-vous conclure ?  
5. Planifiez une expérience **2²** (ou plusieurs) pour analyser l’impact des optimisations. Les optimisations sont-elles effectives ? Expliquez vos résultats.  

   **Pour chaque test 2²** :  
   - Choisissez deux facteurs et deux niveaux.  
   - Calculez l’importance des facteurs via un modèle de régression et interprétez les résultats.

6. Vérifiez si l’ordre d’évaluation des requêtes dans le workload joue un rôle dans l’analyse des performances.
7. Comparez votre système avec **InteGraal** et avec une implémentation concurrente sur des requêtes en étoile.  
   - Quelles conclusions pouvez-vous en tirer ? Expliquez les résultats.  
8. Dans le système concurrent choisi :
   - Les mesures sont-elles réalisées de manière comparable (vérifiez le code source) ?
   - Le système est-il correct et complet ?
9. Toute expérience permettant de mieux comprendre votre système est bienvenue.

---

## Représentation graphique des résultats

1. Présentez les résultats de vos tests dans des histogrammes et analysez-les.  
2. Expliquez les différences de performances entre votre prototype et les systèmes concurrents.

---

## Compilation de WatDiv

### Instructions pour Linux

Utilisez les comptes de la faculté via **x2go**.

**Pour compiler :**  
```bash
make clean
make
```

**Pour générer les données** (le facteur d’échelle 1 génère 15MB de données, 10 -> 150MB, etc.) :

```bash
bin/Release/watdiv -d model/wsdbm-data-model.txt 1
```

Pour sauvegarder les données dans un fichier.
```bash
bin/Release/watdiv -d model/wsdbm-data-model.txt 1 > data.nt
```

**Pour générer des requêtes** :

Ovrez les patrons de requêtes dans le répertoire `testsuite/templates` pour visualiser le fonctionnement du générateur de requêtes.
Modifiez puis exécutez le script `regenerate_queryset.sh` (par exemple, modifiez le nombre de requêtes générées par patron, actuellement fixé à 100).

```bash
. ./regenerate_queryset.sh
```

En cas de problèmes non résolus avec WatDiv, vous pouvez utiliser des données et requêtes mises à disposition [au lien suivant](https://gitlab.etu.umontpellier.fr/p00000013857/watdiv-backup). Attention, il s'agit d'un dépôt de "dépannage" contenant données et requêtes "brutes" qui doivent être traitées avant de pouvoir être utilisées pour l'analyse des performances.