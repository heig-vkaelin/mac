package ch.heig.mac;

import java.util.List;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;


public class Requests {
    private final Cluster cluster;

    public Requests(Cluster cluster) {
        this.cluster = cluster;
    }

    public List<String> getCollectionNames() {
        QueryResult result = cluster.query(
                "SELECT RAW r.name\n" +
                        "FROM system:keyspaces r\n" +
                        "WHERE r.`bucket` = \"mflix-sample\";"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> inconsistentRating() {
        QueryResult result = cluster.query(
                "SELECT imdb.id imdb_id,\n" +
                        "       imdb.rating imdb_rating,\n" +
                        "       tomatoes.viewer.rating tomatoes_rating\n" +
                        "FROM `mflix-sample`.`_default`.`movies`\n" +
                        "WHERE tomatoes.viewer.rating != 0\n" +
                        "    AND ABS(imdb.rating - tomatoes.viewer.rating) > 7;"
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> hiddenGem() {
        QueryResult result = cluster.query(
                "SELECT title\n" +
                        "FROM `mflix-sample`.`_default`.`movies`\n" +
                        "WHERE tomatoes.critic.rating = 10\n" +
                        "    AND tomatoes.viewer.rating IS NOT VALUED;"
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> topReviewers() {
        QueryResult result = cluster.query(
                "SELECT name,\n" +
                        "       COUNT(_id) cnt\n" +
                        "FROM `mflix-sample`.`_default`.`comments`\n" +
                        "GROUP BY name\n" +
                        "ORDER BY cnt DESC\n" +
                        "LIMIT 10;"
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<String> greatReviewers() {
        QueryResult result = cluster.query(
                "SELECT DISTINCT RAW name\n" +
                        "FROM `mflix-sample`.`_default`.`comments`\n" +
                        "GROUP BY name\n" +
                        "HAVING COUNT(_id) > 300;"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> plentifulDirectors() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> confusingMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector1(String director) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    // Returns the number of documents updated.
    public long removeEarlyProjection(String movieId) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> nightMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }


}
