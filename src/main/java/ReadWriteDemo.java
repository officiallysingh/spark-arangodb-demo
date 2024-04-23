import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ReadWriteDemo {

  public static void readWriteDemo() {
    System.out.println("-----------------------");
    System.out.println("--- READ-WRITE DEMO ---");
    System.out.println("-----------------------");

    System.out.println("Reading 'movies' collection and writing 'actionMovies' collection...");
    Dataset<Row> actionMoviesDF =
        ReadDemo.readTable("movies", Schemas.movieSchema())
            .select("_key", "title", "releaseDate", "runtime", "description")
            .filter("genre = 'Action'");
    WriteDemo.saveDF(actionMoviesDF, "actionMovies", Demo.TABLE_TYPE_DOCUMENT);
    /*
      Filters and projection pushdowns are applied in this case.

      In the console an info message log like the following will be printed:
      >  INFO  ArangoScanBuilder:57 - Filters fully applied in AQL:
      >      IsNotNull(genre)
          >     EqualTo(genre,Action)

      Also the generated AQL query will be printed with log level debug:
      >  DEBUG ArangoClient:61 - Executing AQL query:
      >     FOR d IN @@col FILTER `d`.`genre` != null AND `d`.`genre` == "Action" RETURN {`_key`:`d`.`_key`,`description`:`d`.`description`,`releaseDate`:`d`.`releaseDate`,`runtime`:`d`.`runtime`,`title`:`d`.`title`}
        >     with params: Map(@col -> movies)
    */
  }
}
