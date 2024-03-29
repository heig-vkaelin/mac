package ch.heig.mac;

import java.util.List;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
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
        QueryResult result = cluster.query(
                "SELECT imdb.id imdb_id,\n" +
                        "       imdb.rating rating,\n" +
                        "       `cast`\n" +
                        "FROM `mflix-sample`.`_default`.`movies`\n" +
                        "WHERE IS_NUMBER(imdb.rating)\n" +
                        "    AND imdb.rating > 8\n" +
                        "    AND $actor IN `cast`",
                QueryOptions
                        .queryOptions()
                        .parameters(JsonObject.create().put("actor", actor))
        );
        return result.rowsAs(JsonObject.class);
    }
    
    public List<JsonObject> plentifulDirectors() {
        QueryResult result = cluster.query(
                "SELECT director_name,\n" +
                        "       COUNT(m._id) count_film\n" +
                        "FROM `mflix-sample`.`_default`.`movies` m\n" +
                        "UNNEST directors AS director_name\n" +
                        "GROUP BY director_name\n" +
                        "HAVING COUNT(m._id) > 30;"
        );
        return result.rowsAs(JsonObject.class);
    }
    
    public List<JsonObject> confusingMovies() {
        QueryResult result = cluster.query(
                "SELECT _id movie_id,\n" +
                        "       `title` `title`\n" +
                        "FROM `mflix-sample`.`_default`.`movies`\n" +
                        "WHERE ARRAY_COUNT(directors) > 20;"
        );
        return result.rowsAs(JsonObject.class);
    }
    
    public List<JsonObject> commentsOfDirector1(String director) {
        QueryResult result = cluster.query(
                "SELECT c.movie_id,\n" +
                        "       c.text\n" +
                        "FROM `mflix-sample`.`_default`.`comments` c\n" +
                        "WHERE c.movie_id IN (\n" +
                        "    SELECT RAW _id\n" +
                        "    FROM `mflix-sample`.`_default`.`movies` m\n" +
                        "    WHERE $director WITHIN m.directors);",
                QueryOptions
                        .queryOptions()
                        .parameters(JsonObject.create().put("director", director))
        );
        
        return result.rowsAs(JsonObject.class);
    }
    
    public List<JsonObject> commentsOfDirector2(String director) {
        QueryResult result = cluster.query(
                "SELECT m._id movie_id,\n" +
                        "        c.text text\n" +
                        "        FROM `mflix-sample`._default.movies m\n" +
                        "        JOIN `mflix-sample`._default.comments c ON m._id = c.movie_id\n" +
                        "        WHERE $director WITHIN m.directors;",
                QueryOptions
                        .queryOptions()
                        .parameters(JsonObject.create().put("director", director))
        );
        return result.rowsAs(JsonObject.class);
    }
    
    // Returns the number of documents updated.
    public long removeEarlyProjection(String movieId) {
        QueryResult result = cluster.query(
                "UPDATE `mflix-sample`.`_default`.`theaters`\n" +
                        "SET schedule = ARRAY s FOR s IN schedule WHEN s.moveId != movieId\n" +
                        "    OR s.hourBegin >= \"18:00:00\" END\n" +
                        "WHERE $movieId WITHIN schedule;",
                QueryOptions
                        .queryOptions()
                        .parameters(JsonObject.create().put("movieId", movieId))
                        .metrics(true)
        );
        
        return result.metaData().metrics().isPresent() ?
                result.metaData().metrics().get().mutationCount() : 0;
    }
    
    public List<JsonObject> nightMovies() {
        QueryResult result = cluster.query(
                "SELECT _id movie_id,\n" +
                        "       title\n" +
                        "FROM `mflix-sample`.`_default`.`movies`\n" +
                        "WHERE _id IN (\n" +
                        "    SELECT RAW sched.movieId\n" +
                        "    FROM `mflix-sample`.`_default`.`theaters`\n" +
                        "    UNNEST schedule AS sched\n" +
                        "    GROUP BY sched.movieId\n" +
                        "    HAVING MIN(sched.hourBegin) >= \"18:00:00\");"
        );
        
        return result.rowsAs(JsonObject.class);
    }
}
