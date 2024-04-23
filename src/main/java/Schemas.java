import org.apache.spark.sql.types.*;

public class Schemas {

  public static StructType movieSchema() {
    return DataTypes.createStructType(
        new StructField[] {
          DataTypes.createStructField("_id", DataTypes.StringType, false),
          DataTypes.createStructField("_key", DataTypes.StringType, false),
          DataTypes.createStructField("description", DataTypes.StringType, true),
          DataTypes.createStructField("genre", DataTypes.StringType, true),
          DataTypes.createStructField("homepage", DataTypes.StringType, true),
          DataTypes.createStructField("imageUrl", DataTypes.StringType, true),
          DataTypes.createStructField("imdbId", DataTypes.StringType, true),
          DataTypes.createStructField("language", DataTypes.StringType, true),
          DataTypes.createStructField("lastModified", DataTypes.TimestampType, true),
          DataTypes.createStructField("releaseDate", DataTypes.DateType, true),
          DataTypes.createStructField("runtime", DataTypes.IntegerType, true),
          DataTypes.createStructField("studio", DataTypes.StringType, true),
          DataTypes.createStructField("tagline", DataTypes.StringType, true),
          DataTypes.createStructField("title", DataTypes.StringType, true),
          DataTypes.createStructField("trailer", DataTypes.StringType, true)
        });
  }

  public static StructType personSchema() {
    return DataTypes.createStructType(
        new StructField[] {
          DataTypes.createStructField("_id", DataTypes.StringType, false),
          DataTypes.createStructField("_key", DataTypes.StringType, false),
          DataTypes.createStructField("biography", DataTypes.StringType, true),
          DataTypes.createStructField("birthday", DataTypes.DateType, true),
          DataTypes.createStructField("birthplace", DataTypes.StringType, true),
          DataTypes.createStructField("lastModified", DataTypes.TimestampType, true),
          DataTypes.createStructField("name", DataTypes.StringType, true),
          DataTypes.createStructField("profileImageUrl", DataTypes.StringType, true)
        });
  }

  public static StructType actsInSchema() {
    return DataTypes.createStructType(
        new StructField[] {
          DataTypes.createStructField("_id", DataTypes.StringType, false),
          DataTypes.createStructField("_key", DataTypes.StringType, false),
          DataTypes.createStructField("_from", DataTypes.StringType, false),
          DataTypes.createStructField("_to", DataTypes.StringType, false),
          DataTypes.createStructField("name", DataTypes.StringType, true)
        });
  }

  public static StructType directedSchema() {
    return DataTypes.createStructType(
        new StructField[] {
          DataTypes.createStructField("_id", DataTypes.StringType, false),
          DataTypes.createStructField("_key", DataTypes.StringType, false),
          DataTypes.createStructField("_from", DataTypes.StringType, false),
          DataTypes.createStructField("_to", DataTypes.StringType, false)
        });
  }
}
