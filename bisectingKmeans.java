// BISECTING K MEANS ALGORTIHM USING SPARK


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

public class bisectingKmeans {
   int count ;
   public static ArrayList<Double> intoDoubles(String str){
    String [] arr = str.split(" ") ;
    ArrayList<Double> res = new ArrayList<Double>();
     for(int i =0 ; i < arr.size() ; i++){
      res.add(Double.parseDouble(arr[i]));
     }
     count ++;
     return res;
   }
   
   public static void main(String[] args) throws Exception {
       if (args.length < 5) {
      System.err.println("Usage: bisectingKmeans<k-bisectingKmeans> <d-dimension> <R> <S> <output-path>");
      System.exit(1);
      }
   
    Integer k = Integer.valueOf(args[0]); 
    Integer d = Integer.valueOf(args[1]); 
    String datasetR = args[2];
    String outputPath = args[3];
    int no = k.getValue();
    SparkSession session = SparkSession
      .builder()
      .appName("bisectingKmeans")
      .getOrCreate();

    JavaSparkContext context = JavaSparkContext.fromSparkContext(session.sparkContext());
    
    final Broadcast<Integer> broadcastK = context.broadcast(k);
    final Broadcast<Integer> broadcastD = context.broadcast(d);

    JavaRDD<String> R = session.read().textFile(datasetR).javaRDD();
    R.saveAsTextFile(outputPath+"/R");  

	//RDD for initial dataset
	JavaPairRDD<String,String> cart =  
      R.mapToPair(new PairFunction<String , String,String>(){
        @Override
        public Tuple2<String,String> call(String cartRecord) {
          String index = new String("0") ;
          return new Tuple2<String,String>>(index, cartRecord);
        }
    });
    int count = 0 ;

    while(count !=  no){
      JavaPairRDD<String,String> sortedCart = cart.sortByKey(); //initial Dataset sorted according to clusterid/index
      JavaPairRDD<String,String> sortedCartCopy = cart.sortByKey(); //copy of above RDD

      HashMap<String,Integer> count =  sortedCart.countByKey();

      Map.Entry<String,Integer> maxEntry = null;
      for (Map.Entry<String,Integer> entry : count.entrySet())
      {
        if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0)
        {
            maxEntry = entry;
        }
      }
      Function<Tuple2<String, String>, Boolean> longFilter =
          new Function<Tuple2<String, String>, Boolean>() {
              public Boolean call(Tuple2<String, String> keyValue) {
            return (keyValue._1()  == entry.getKey());
        }
      };

      JavaPairRDD<String, String> result1 = sortedCart.filter(longFilter); //RDD of largest cluster present


      Function<Tuple2<String, String>, Boolean> longFilter2 =
          new Function<Tuple2<String, String>, Boolean>() {
              public Boolean call(Tuple2<String, String> keyValue) {
            return (keyValue._1()  != entry.getKey());
        }
      };
      int kmeansIteration =0 ;

      // K means with k=2 on cluster obtained in previous step
      for(kmeansIteration ;kmeansIteration<10; kmeansIteration++) {
          JavaPairRDD<String, String> result2 = sortedCart.filter(longFilter2);
          int countFraction =  result2.count();
          float fraction =  2.0/countFraction ;
          JavaPairRDD<String, String> center = sample(false,fraction,new Random(System.currentTimeMillis()));
          JavaRDD<String> result1Single = result1.values();
          JavaRDD<String> centerSingle = center.values();
          JavaPairRDD<String,String> cat = result1.cartesian(center);

          // RDD from cluster assignment
          JavaPairRDD<String,Tuple2<Double,String>> kmeansMapped =
                //                              input                  K       V
                cat.mapToPair(new PairFunction<Tuple2<String,String>, String, Tuple2<Double,String>>() {
          @Override
          public Tuple2<String,Tuple2<Double,String>> call(Tuple2<String,String> cartRecord) {
          String rRecord = cartRecord._1;
          String sRecord = cartRecord._2;
          String[] rTokens = rRecord.split(" "); 
          String[] sTokens = sRecord.split(" "); 
           
          ArrayList<Double> rD = new ArrayList<Double>();
          for(int i = 0 ; i < rTokens.size; i++)
            rD.add(Double.parseDouble(rTokens[i]));
          
          ArrayList<Double> sD = new ArrayList<Double>();
          for(int i = 0 ; i < sTokens.size ; i++)
            sD.add(Double.parseDouble(sTokens[i]));
          Integer d = broadcastD.value();
          double distance = 0.0;
          for (int i = 0; i < d; i++) {
          double difference = rD[i] - sD[i];
          distance += difference * difference;
          }
        
          distance=Math.sqrt(distance);
            String K = rRecord ;
            Tuple2<Double,String> V = new Tuple2<Double,String>(distance, sRecord);
            return new Tuple2<String,Tuple2<Double,String>>(K, V);
          }
        });
        
        // RDD with clusters grouped
        JavaPairRDD<String, Iterable<Tuple2<Double,String>>> kmeansGrouped = kmeansMapped.groupByKey();
        JavaPairRDD<String, String> kmeansOutput =
            kmeansGrouped.mapValues(new Function<Iterable<Tuple2<Double,String>>,String>() {
          @Override
          public String call(Iterable<Tuple2<Double,String>> neighbors) {
              Integer k = 1; broadcastK.value();
              SortedMap<Double, String>  nearestK = new TreeMap<Double, String>();
                for (Tuple2<Double,String> neighbor : neighbors) {
                Double distance = neighbor._1;
                String sRecord =  neighbor._2;
                nearestK.put(distance, sRecord);
                if (nearestK.size() > k) {
                   nearestK.remove(nearestK.lastKey());
                }
              }

          Map<String, Integer> majority = new HashMap<String, Integer>();
          for (Map.Entry<Double, String> entry : nearestK.entrySet()) {
            String sRecord = entry.getValue();
            Integer temp2 = majority.get(sRecord);
            if (temp2 == null){
               majority.put(sRecord, 1);
            }
            else {
               majority.put(sRecord, temp2+1);
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
                int temp = entry.getValue();
                if (temp > votes) {
                    selectedClassification = entry.getKey();
                    votes = temp;
                }
              }
            }
          
        }
        }); 
      
        }
        String[] arr  = center.take(2);
        Function<Tuple2<String, String>, Boolean> longFilter3 =
            new Function<Tuple2<String, String>, Boolean>() {
                public Boolean call(Tuple2<String, String> keyValue) {
              return (keyValue._2()  == arr[0]);
          }
        };
        JavaPairRDD<String,String> cluster1 = kmeansOutput.filter(longFilter3); 


        Function<Tuple2<String, String>, Boolean> longFilter4 =
            new Function<Tuple2<String, String>, Boolean>() {
                public Boolean call(Tuple2<String, String> keyValue) {
              return (keyValue._2()  == arr[1]);
          }
        };
        JavaPairRDD<String,String> cluster2 = kmeansOutput.filter(longFilter4);

        JavaRDD<double[]> whatYouWantRdd = cluster1.map(new Function<String, double[]>() {
        @Override
          public double[] call(Tuple2<String,String> clusterRecord) throws Exception {
            return splitStringtoDoubles(clusterRecord._1());
          }

          private double[] splitStringtoDoubles(String s) {
          String[] splitVals = s.split(" ");
          Double[] vals = new Double[splitVals.length];
          for(int i=0; i < splitVals.length; i++) {
              vals[i] = Double.parseDouble(splitVals[i]);
          }
          return vals;
        }
        });

        List<double[]> whatYouWant = whatYouWantRdd.collect();
        double[] new_w = {0,0,0,0,0,0,0,0,0,0,0,0,0};
        for(int i = 0 ; i <whatYouWant.size();  i++){
          for(int j = 0 ; j <13 ; j++){
            new_w[j] += whatYouWant.get(i)[j];
          }
        }

        for(int i = 0 ; i <12; i++){
          new_w[i]=/13;
        }
        String new_center = Arrays.stream(new_w)
          .mapToObj(String::valueOf)
          .collect(Collectors.joining(" "));

        JavaRDD<double[]> whatYouWantRdd2 = cluster2.map(new Function<String, double[]>() {
          @Override
          public double[] call(Tuple2<String,String> clusterRecord) throws Exception {
            return splitStringtoDoubles(clusterRecord._1());
          }

          private double[] splitStringtoDoubles(String s) {
          String[] splitVals = s.split("\\t");
          Double[] vals = new Double[splitVals.length];
          for(int i=0; i < splitVals.length; i++) {
              vals[i] = Double.parseDouble(splitVals[i]);
          }
          return vals;
        }
        });

        List<double[]> whatYouWant2 = whatYouWantRdd2.collect();
        double[] new_w2 = {0,0,0,0,0,0,0,0,0,0,0,0,0};
        for(int i = 0 ; i <whatYouWant.size();  i++){
          for(int j = 0 ; j <13 ; j++){
            new_w2[j] += whatYouWant2.get(i)[j];
          }
        }

        for(int i = 0 ; i <12; i++){
          new_w2[i]=/13;
        }
        String new_center2 = Arrays.stream(new_w2)
          .mapToObj(String::valueOf)
          .collect(Collectors.joining(" "));

        centerSingle = context.parallelize(Arrays.asList(new_center, new_center2))
        }

        Java<String> key1 =  cluster1.keys();
        JavaPairRDD<String,String> cart1 =  
        R.mapToPair(new PairFunction<String , String,String>(){
          @Override
          public Tuple2<String,String> call(String cartRecord) {
            String index = new String(entry.getKey()) ;
            return new Tuple2<String,String>>(index, cartRecord);
          }
        });

        JavaPairRDD<String,String> cart2 =  
        R.mapToPair(new PairFunction<String , String,String>(){
          @Override
          public Tuple2<String,String> call(String cartRecord) {
            String index = new String(count + 1) ;
            return new Tuple2<String,String>>(index, cartRecord);
          }
        });

      cart  = context.union(cart,cart1,cart2);
      cart.collect() //assigning next cluster for bisecting further

      count ++;
    }
    JavaPairRDD<String,String> cart = R.cartesian(S);
    cart.saveAsTextFile(outputPath+"/cart");
    
    
    session.stop();
    System.exit(0);
}

