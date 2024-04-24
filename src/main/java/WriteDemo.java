import static org.apache.spark.sql.functions.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class WriteDemo {

  private static final String[] PERSON_FIELD_NAMES = {
    "_key", "name", "releaseDate", "birthday", "lastModified", "type"
  };
  private static final String[] MOVIE_FIELD_NAMES = {
    "_key", "title", "releaseDate", "lastModified", "type"
  };
  private static final String[] DIRECTED_FIELD_NAMES = {"_key", "_from", "_to", "`$label`"};
  private static final String[] ACTED_IN_FIELD_NAMES = {"_key", "_from", "_to", "`$label`"};

  private static final SparkSession spark =
      SparkSession.builder().appName("WriteDemo").master("local[*]").getOrCreate();

  private static final Map<String, String> SAVE_OPTIONS = new HashMap<>();

  static {
    SAVE_OPTIONS.put("table.shards", "9");
    SAVE_OPTIONS.put("confirmTruncate", "true");
    SAVE_OPTIONS.put("overwriteMode", "replace");
  }

  public static void writeDemo() {
    System.out.println("------------------");
    System.out.println("--- WRITE DEMO ---");
    System.out.println("------------------");

    System.out.println("Reading JSON files...");
    Dataset<Row> nodesDF =
        spark
            .read()
            .json(Demo.IMPORT_PATH + "/nodes.jsonl")
            .withColumn("_key", functions.col("_key").isNotNull())
            .withColumn("releaseDate", unixTsToSparkDate(col("releaseDate")))
            .withColumn("birthday", unixTsToSparkDate(col("birthday")))
            .withColumn("lastModified", unixTsToSparkTs(col("lastModified")))
            .persist();
    Dataset<Row> edgesDF =
        spark
            .read()
            .json(Demo.IMPORT_PATH + "/edges.jsonl")
            .withColumn("_key", functions.col("_key").isNotNull())
            .withColumn("_from", functions.concat(lit("persons/"), col("_from")).isNotNull())
            .withColumn("_to", functions.concat(lit("movies/"), col("_to")).isNotNull())
            .persist();

    Dataset<Row> personsDF = nodesDF.selectExpr(PERSON_FIELD_NAMES).filter("type = 'Person'");
    Dataset<Row> moviesDF = nodesDF.selectExpr(MOVIE_FIELD_NAMES).filter("type = 'Movie'");
    Dataset<Row> directedDF =
        edgesDF.selectExpr(DIRECTED_FIELD_NAMES).filter("`$label` = 'DIRECTED'");
    Dataset<Row> actedInDF =
        edgesDF.selectExpr(ACTED_IN_FIELD_NAMES).filter("`$label` = 'ACTS_IN'");

    System.out.println("Writing 'persons' collection...");
    saveDF(personsDF, "persons", Demo.TABLE_TYPE_DOCUMENT);

    System.out.println("Writing 'movies' collection...");
    saveDF(moviesDF, "movies", Demo.TABLE_TYPE_DOCUMENT);

    System.out.println("Writing 'directed' edge collection...");
    saveDF(directedDF, "directed", Demo.TABLE_TYPE_EDGE);

    System.out.println("Writing 'actedIn' edge collection...");
    saveDF(actedInDF, "actedIn", Demo.TABLE_TYPE_EDGE);
  }

  private static Column unixTsToSparkTs(Column c) {
    ////    return functions.expr("CAST(" + c.toString() + " AS TIMESTAMP) / 1000");
    return from_unixtime(c);
  }

  private static Column unixTsToSparkDate(Column c) {
    return unixTsToSparkTs(c).cast(DataTypes.DateType);
  }

  static void saveDF(Dataset<Row> df, String tableName, String tableType) {
    df.write()
        .mode(SaveMode.Overwrite)
        .format("com.arangodb.spark")
        .options(Demo.OPTIONS)
        .options(SAVE_OPTIONS)
        .option("table", tableName)
        .option("table.type", tableType)
        .save();
  }

  public static void main(String[] args) {
    writeDemo();
    spark.stop();
  }
}
