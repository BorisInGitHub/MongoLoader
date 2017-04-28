# Mongo data bulk insertion

## Mode opératoire

###Pour compiler
```bash
mvn clean install
```

PS : Le fichier logback.xml est dans les main resources, donc intégré dans la librairie JAR.

###Pour tester
 
Lancement de Mongo dans un docker
```bash
docker run -i -t --name mongoDocker -h mongoDocker -p 27017:27017 --net="bridge" mongo:latest --quiet --nojournal
```

Lancement du tester

```bash
java -jar target/MongoLoader-1.0-SNAPSHOT-jar-with-dependencies.jar mongodb://localhost:27017/ /pathTo/test.csv 
```


## Optimisations possibles
* Créer les indexs après le chargement des données
* insertMany (à priori remplace le mode Bulk, mais je n'ai pas vu de différence)
* Lecture du CSV (hors sujet dans cette étude)
* Plusieurs threads pour lire ou insérer les données ...

