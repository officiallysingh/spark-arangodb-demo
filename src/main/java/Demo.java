import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public class Demo {

  static final String IMPORT_PATH =
      System.getProperty("importPath", "spark-arangodb-demo/src/main/resources/import");
  static final String TABLE_TYPE_DOCUMENT = "document";
  static final String TABLE_TYPE_EDGE = "edge";

  private static final String DATABASE = System.getProperty("database", "test_db");
  private static final String PASSWORD = System.getProperty("password", "admin");
  //  private static final String ENDPOINTS =
  //      System.getProperty("endpoints", "172.28.0.1:8529,172.28.0.1:8539,172.28.0.1:8549");
  private static final String ENDPOINTS = System.getProperty("endpoints", "localhost:8529");
  private static final String SSL_ENABLED = System.getProperty("ssl.enabled", "false");
  private static final String SSL_CERT_VALUE = System.getProperty("ssl.cert.value", "");

  static final SparkSession spark =
      SparkSession.builder()
          .appName("arangodb-demo")
          .master("local[*]")
          .config("spark.executor.instances", "3")
          .getOrCreate();

  static final Map<String, String> OPTIONS = new HashMap<>();

  static {
    OPTIONS.put("database", DATABASE);
    OPTIONS.put("password", PASSWORD);
    OPTIONS.put("endpoints", ENDPOINTS);
    OPTIONS.put("ssl.enabled", SSL_ENABLED);
    OPTIONS.put("ssl.cert.value", SSL_CERT_VALUE);
  }

  public static void main(String[] args) {
    WriteDemo.writeDemo();
    ReadDemo.readDemo();
//    ReadWriteDemo.readWriteDemo();
    spark.stop();
  }
}
