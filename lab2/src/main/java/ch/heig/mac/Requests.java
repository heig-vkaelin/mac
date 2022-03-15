package ch.heig.mac;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

public class Requests {
    private static final Logger LOGGER = Logger.getLogger(Requests.class.getName());
    private final Driver driver;
    
    public Requests(Driver driver) {
        this.driver = driver;
    }
    
    public List<String> getDbLabels() {
        var dbVisualizationQuery = "CALL db.labels";
        
        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list(t -> t.get("label").asString());
        }
    }

    // TODO: query surement cheloue (les comparaison sont correctes ?) à demander
    public List<Record> possibleSpreaders() {
        var query =
                "MATCH(sick:Person{healthstatus:'Sick'})-[v1:VISITS]->(p:Place)<-[v2:VISITS]-" +
                        "(healthy:Person{healthstatus:'Healthy'})\n" +
                        "WITH sick, v1, v2, Count(healthy) AS atLeastOne\n" +
                        "WHERE v1.starttime > sick.confirmedtime AND v2.starttime > sick.confirmedtime AND " +
                        "atLeastOne > 0\n" +
                        "RETURN DISTINCT sick.name AS sickName";

        try (var session = driver.session()) {
            var result = session.run(query);
            return result.list();
        }
    }
    
    public List<Record> possibleSpreadCounts() {
        var query =
                "MATCH(sick:Person{healthstatus:'Sick'})-[v1:VISITS]->(p:Place)<-[v2:VISITS]-" +
                        "(healthy:Person{healthstatus:'Healthy'}) " +
                        "WHERE v1.starttime > sick.confirmedtime AND " +
                        "v2.starttime > sick.confirmedtime " +
                        "RETURN sick.name AS sickName, COUNT(healthy) AS nbHealthy";
        
        try (var session = driver.session()) {
            var result = session.run(query);
            return result.list();
        }
    }
    
    public List<Record> carelessPeople() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }
    
    public List<Record> sociallyCareful() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }
    
    public List<Record> peopleToInform() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }
    
    public List<Record> setHighRisk() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }
    
    public List<Record> healthyCompanionsOf(String name) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }
    
    public Record topSickSite() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }
    
    public List<Record> sickFrom(List<String> names) {
        var query =
                "MATCH (p:Person{healthstatus:'Sick'}) WHERE p.name IN " +
                        "$names" +
                        " RETURN p.name AS sickName";
        
        try (var session = driver.session()) {
            var result = session.run(
                    query,
                    Values.parameters("names", names)
            );
            return result.list();
        }
    }
}
