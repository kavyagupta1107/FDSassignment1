// KNN ALGORITHM USING SPARK


import java.util.Map;
import java.util.SortedMap;
import scala.Tuple2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

public class kNNSpark{
   
   public static void main(String[] args) throws Exception {
    if (args.length<5) { System.err.println("Please provide correct input in the form: k-knn d-dimension testingDataFile testingDataFile output-path");
      System.exit(1);
    }
 
    Integer k = Integer.valueOf(args[0]); 
    Integer d = Integer.valueOf(args[1]); // number of attributes
    String datasetR = args[2];
    String datasetS = args[3];
    String outputPath = args[4];
     
    SparkSession session = SparkSession.builder().appName("knn").getOrCreate();

    JavaSparkContext context = JavaSparkContext.fromSparkContext(session.sparkContext());
    
    final Broadcast<Integer> broadcastK = context.broadcast(k);
    final Broadcast<Integer> broadcastD = context.broadcast(d);

    JavaRDD<String> test= session.read().textFile(datasetR).javaRDD();
    test.saveAsTextFile(outputPath+"/test");  
    JavaRDD<String> train = session.read().textFile(datasetS).javaRDD();
    train.saveAsTextFile(outputPath+"/train");

    JavaPairRDD<String,String> cart = test.cartesian(train);
    cart.saveAsTextFile(outputPath+"/cart");
    
    JavaPairRDD<String,Tuple2<Double,Strilng>> knnMapped =
      cart.mapToPair(new PairFunction<Tupe2<String,String>, String, Tuple2<Double,String>>() {
      @Override
      public Tuple2<String,Tuple2<Double,String>> call(Tuple2<String,String> cartRecord) {
        String rRecord = cartRecord._1;
        String sRecord = cartRecord._2;
        String[] rTokens = rRecord.split(" "); 
        String[] sTokens = sRecord.split(" "); 
        String sClassificationID = sTokens[sTokens.size -1]; 
        ArrayList<Double> rD = new ArrayList<Double>();
        for(int i = 0 ; i < rTokens.size -1; i++)
          rD.add(Double.parseDouble(rTokens[i]));
        
        ArrayList<Double> sD = new ArrayList<Double>();
        for(int i = 0 ; i < sTokens.size -1; i++)
          sD.add(Double.parseDouble(sTokens[i]));


        
        Integer d = broadcastD.value();
        double distance = 0.0;
      for (int i = 0; i < d; i++) {
        double difference = rD[i] - sD[i];
        distance += difference * difference;
      }
      
      distance=Math.sqrt(distance);
        String K = rRecord ;        Tuple2<Double,String> V = new Tuple2<Double,String>(distance, sClassificationID);
        return new Tuple2<String,Tuple2<Double,String>>(K, V);
      }
    });
    knnMapped.saveAsTextFile(outputPath+"/knnMapped");    

    JavaPairRDD<String, Iterable<Tuple2<Double,String>>> knnGrouped = knnMapped.groupByKey();
    
    JavaPairRDD<String, String> knnOutput =
        knnGrouped.mapValues(new Function<Iterable<Tuple2<Double,String>>,String>() {
      @Override
      public String call(Iterable<Tuple2<Double,String>> neighbors) {
          Integer k = broadcastK.value();
          SortedMap<Double, String>  nearestK = new TreeMap<Double, String>();
            for (Tuple2<Double,String> neighbor : neighbors) {
            Double distance = neighbor._1;
            String classificationID =  neighbor._2;
            nearestK.put(distance, classificationID);
            if (nearestK.size() > k) {
               nearestK.remove(nearestK.lastKey());
            }
          }

          Map<String, Integer> majority = new HashMap<String, Integer>();
          for (Map.Entry<Double, String> entry : nearestK.entrySet()) {
            String classificationID = entry.getValue();
            Integer count = majority.get(classificationID);
            if (count == null){
               majority.put(classificationID, 1);
            }
            else {
               majority.put(classificationID, count+1);
            }
          } 
          
            int votes = 0;
            String selectedClassification = null;
            for (Map.Entry<String, Integer> entry : majority.entrySet()) {
              if (selectedClassification == null) {
                  selectedClassification = entry.getKey();
              votes = entry.getValue();
              }
              else {
                int count = entry.getValue();
                if (count > votes) {
                    selectedClassification = entry.getKey();
                    votes = count;
                }
              }
            }
          return selectedClassification;
      }
    }); 
    knnOutput.saveAsTextFile(outputPath+"/knnOutput");

    session.stop();
    System.exit(0);
  }
}