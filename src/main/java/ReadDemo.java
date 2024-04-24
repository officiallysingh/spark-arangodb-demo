import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

public class ReadDemo {

  public static void readDemo() {
    System.out.println("-----------------");
    System.out.println("--- READ DEMO ---");
    System.out.println("-----------------");

    Dataset<Row> moviesDF = readTable("movies", Schemas.movieSchema());

    System.out.println(
        "Read table: history movies or documentaries about 'World War' released from 2000-01-01");
    moviesDF
        .select("title", "releaseDate", "genre", "description")
        .filter(
            "genre IN ('History', 'Documentary') AND description LIKE '%World War%' AND releaseDate > '2000'")
        .show(20, 200);

    System.out.println(
        "Read query: actors of movies directed by Clint Eastwood with related movie title and interpreted role");
    readQuery(
            "WITH movies, persons\n"
                + "FOR v, e, p IN 2 ANY 'persons/1062' OUTBOUND directed, INBOUND actedIn\n"
                + "   RETURN {movie: p.vertices[1].title, name: v.name, role: p.edges[1].name}",
            new StructType(
                new StructField[] {
                  new StructField("movie", DataTypes.StringType, false, Metadata.empty()),
                  new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                  new StructField("role", DataTypes.StringType, false, Metadata.empty())
                }))
        .show(20, 200);
  }

  static Dataset<Row> readTable(String tableName, StructType schema) {
    return Demo.spark
        .read()
        .format("com.arangodb.spark")
        .options(Demo.OPTIONS)
        .option("table", tableName)
        .schema(schema)
        .load();
  }

  private static Dataset<Row> readQuery(String query, StructType schema) {
    return Demo.spark
        .read()
        .format("com.arangodb.spark")
        .options(Demo.OPTIONS)
        .option("query", query)
        .schema(schema)
        .load();
  }
}
