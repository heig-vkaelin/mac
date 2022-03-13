package ch.heig.mac;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.java.Cluster;

public class Indices {
    private final Cluster cluster;
    
    protected Map<String, List<String>> requiredIndices = Map.ofEntries(
            Map.entry(
                    "commentsOfDirector2",
                    List.of("CREATE INDEX idx_comments_movie_id ON `mflix-sample`._default.comments(movie_id);")
            )
    );
    
    public Indices(Cluster cluster) {
        this.cluster = cluster;
    }
    
    private void createIndex(String createQuery) {
        try {
            cluster.query(createQuery);
        } catch (IndexExistsException ex) {
            // Ignore already existing index
            // You may need to manually delete old indices if you change them in this class after executing this method
        }
    }
    
    public void createRequiredIndicesOf(String questionMethodName) {
        requiredIndices.getOrDefault(questionMethodName, List.of()).forEach(this::createIndex);
    }
    
    public void createRequiredIndices() {
        requiredIndices.values()
                .stream()
                .flatMap(Collection::stream)
                .forEach(this::createIndex);
    }
    
    public Set<String> getMethodNamesOfRequiredIndices() {
        return requiredIndices.keySet();
    }
}
