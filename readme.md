# Mongo data insertion

Pour tester : 
```bash
docker run -i -t --name mongoDocker -h mongoDocker -p 27017:27017 --net="bridge" mongo:latest --quiet --nojournal
```

puis un 

```bash
java -jar target/MongoLoader-1.0-SNAPSHOT-jar-with-dependencies.jar mongodb://localhost:27017/ /pathTo/test.csv 
```
