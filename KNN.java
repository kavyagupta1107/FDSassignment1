// KNN ALGORITHM USING HADOOP MAP REDUCE

import java.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNN{

	public static class DoubleDouble implements WritableComparable<DoubleDouble>{
		private Double distance = 0.0 ;

		private Double model = 0.0;

			public void set(Double lhs, Double rhs)
			{
				distance = lhs;
				model = rhs;
			}
			
			public Double getDistance()
			{
				return distance;
			}
			
			public Double getModel()
			{
				return model;
			}
			
			@Override
			public void write(DataOutput out) throws IOException
			{
				out.writeDouble(distance);
				out.writeDouble(model);
			}
			
			@Override
			public int compareTo(DoubleDouble o)
			{
				return (this.model).compareTo(o.model);
			}
		}

		public static class KnnMapper extends Mapper<Object, Text, NullWritable, DoubleDouble>
	{
		DoubleDouble distanceAndModel = new DoubleDouble();
		TreeMap<Double, Double> KnnMap = new TreeMap<Double, Double>();
		
	
		int K;
	    
		double a1;
		double a2;
		double a3;
		double a4;
		double a5;
		double a6;
		double a7;
		double a8;
		double a9;
		
		
		double normaliseda1;
		double normaliseda2;
		double normaliseda3;
		double normaliseda4;
		double normaliseda5;
		double normaliseda6;
		double normaliseda7;
		double normaliseda8;
		double normaliseda9;
		
		// The known ranges of the dataset, which can be hardcoded in for the purposes of this example
		double minA1 = 18;
		double maxA1 = 77;
		double minA2 = 18;
		double maxA2 = 77;
		double minA3 = 18;
		double maxA3 = 77;
		double minA4 = 18;
		double maxA4 = 77;
		double minA5 = 18;
		double maxA5 = 77;
		double minA6 = 18;
		double maxA6 = 77;
		double minA7 = 18;
		double maxA7 = 77;
		double minA8 = 18;
		double maxA8 = 77;
		double minA9 = 18;
		double maxA9= 77;

		
		private double getDoubleFrom(String n1, double minValue, double maxValue)
		{
			return (Double.parseDouble(n1) - minValue) / (maxValue - minValue);
		}
		
	    private double distanceSquare(double n1)
		{
			return Math.pow(n1,2);
		}
		

		public double totalSquaredDistance(double A1, double A2,double A3, double A4, double A5, double A6, double A7, double A8,double A9,double B1, double B2,double B3, double B4, double B5, double B6, double B7, double B8,double B9)
		{	
			double a1 = A1-B1;
			double a2 = A2-B2;
			double a3 = A3-B3;
			double a4 = A4-B4;
			double a5 = A5-B5;
			double a6 = A6-B6;
			double a7 = A7-B7;
			double a8 = A8-B8;
			double a9 = A9-B9;
			return distanceSquare(a1) + distanceSquare(a2) +distanceSquare(a3) +distanceSquare(a4) +distanceSquare(a5) +distanceSquare(a6) +distanceSquare(a7) +distanceSquare(a8)  ;
		}
		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0)
			{
				
				String knnParams = FileUtils.readFileToString(new File("./knnParamFile")); //to be changed
				StringTokenizer st = new StringTokenizer(knnParams, " ");
		    	
		    	K =  10; 
		    	double normaliseda1 = getDoubleFrom(st.nextToken(),minA1,maxA1) ;
				double normaliseda2 = getDoubleFrom(st.nextToken(),minA2,maxA2);
				double normaliseda3 = getDoubleFrom(st.nextToken(),minA3,maxA3);
				double normaliseda4 = getDoubleFrom((st.nextToken()),minA4,maxA4) ;
				double normaliseda5 = getDoubleFrom((st.nextToken()),minA5,maxA5) ;
				double normaliseda6 = getDoubleFrom((st.nextToken()),minA6,maxA6) ;
				double normaliseda7 = getDoubleFrom((st.nextToken()),minA7,maxA7) ;
				double normaliseda8 = getDoubleFrom((st.nextToken()),minA8,maxA8) ;
				double normaliseda9 = getDoubleFrom((st.nextToken()),minA9,maxA9) ;
				
			}
		}




		@Override
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String rLine = value.toString();
			StringTokenizer st = new StringTokenizer(rLine, " " );

				double a1 = getDoubleFrom(st.nextToken(),minA1,maxA1) ;
				double a2 = getDoubleFrom(st.nextToken(),minA2,maxA2);
				double a3 = getDoubleFrom(st.nextToken(),minA3,maxA3);
				double a4 = getDoubleFrom((st.nextToken()),minA4,maxA4) ;
				double a5 = getDoubleFrom((st.nextToken()),minA5,maxA5) ;
				double a6 = getDoubleFrom((st.nextToken()),minA6,maxA6) ;
				double a7 = getDoubleFrom((st.nextToken()),minA7,maxA7) ;
				double a8 = getDoubleFrom((st.nextToken()),minA8,maxA8) ;
				double a9 = getDoubleFrom((st.nextToken()),minA9,maxA9) ;
			double rModel = Double.parseDouble(st.nextToken());
			double tDist = totalSquaredDistance(normaliseda1,normaliseda2,normaliseda3,normaliseda4,normaliseda5,normaliseda6,normaliseda7,normaliseda8,normaliseda9,a1,a2,a3,a4,a5,a6,a7,a8,a9 );		
			
			KnnMap.put(tDist, rModel);
			if (KnnMap.size() > K)
			{
				KnnMap.remove(KnnMap.lastKey());
			}
		}
        
        @Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
		
			for(Map.Entry<Double, Double> entry : KnnMap.entrySet())
			{
				  Double knnDist = entry.getKey();
				  Double knnModel = entry.getValue();
		          distanceAndModel.set(knnDist, knnModel);
				  context.write(NullWritable.get(), distanceAndModel);
			}
		}


	}

    public static class KnnReducer extends Reducer<NullWritable, DoubleDouble, NullWritable, Text>
	{
		TreeMap<Double, Double> KnnMap = new TreeMap<Double, Double>();
		int K;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0)
			{
				String knnParams = FileUtils.readFileToString(new File("./knnParamFile"));
				StringTokenizer st = new StringTokenizer(knnParams, " ");
				K = 10 ;
			}
		}

		@Override
		public void reduce(NullWritable key, Iterable<DoubleDouble> values, Context context) throws IOException, InterruptedException
		{
			for (DoubleDouble val : values)
			{
				double rModel = val.getModel();
				double tDist = val.getDistance();
			
				KnnMap.put(tDist, rModel);
				if (KnnMap.size() > K)
				{
					KnnMap.remove(KnnMap.lastKey());
				}
			}	

				List<Double> knnList = new ArrayList<Double>(KnnMap.values());

				Map<Double, Integer> freqMap = new HashMap<Double, Integer>();
			    
			    for(int i=0; i< knnList.size(); i++)
			    {  
			        Integer frequency = freqMap.get(knnList.get(i));
			        if(frequency == null)
			        {
			            freqMap.put(knnList.get(i), 1);
			        } else
			        {
			            freqMap.put(knnList.get(i), frequency+1);
			        }
			    }
			    
			    
			    Double mostCommonModel = null;
			    int maxFrequency = -1;
			    for(Map.Entry<Double, Integer> entry: freqMap.entrySet())
			    {
			        if(entry.getValue() > maxFrequency)
			        {
			            mostCommonModel = entry.getKey();
			            maxFrequency = entry.getValue();
			        }
			    }
			    
			
			context.write(NullWritable.get(), new Text(Double.toString(mostCommonModel))); // Use this line to produce a single classification
//			
		}
	}

    public static void main(String[] args) throws Exception {
         Configuration conf = new Configuration();
		if (args.length != 3) {
        System.err.println("Please provide input of the form <in> <out> <parameter file>");
        System.exit(2);
    }

    try (final BufferedReader br = new BufferedReader(new FileReader(args[2]))) {
        int n = 1;
            String line;
        	while ((line = br.readLine()) != null) {
            final File tmpDataFile = File.createTempFile("hadoop-test-", null);
            try (BufferedWriter tmpDataWriter = new BufferedWriter(new FileWriter(tmpDataFile))) {
                tmpDataWriter.write(line);
                tmpDataWriter.flush();
            }

            Job job = Job.getInstance(conf, "Find K-Nearest Neighbour #" + n);
            job.setJarByClass(KNN.class);
            job.addCacheFile(new URI(tmpDataFile.getAbsolutePath() + "#knnParamFile")); // Parameter file containing test data

            job.setMapperClass(KnnMapper.class);
            job.setReducerClass(KnnReducer.class);
            job.setNumReduceTasks(1); 

            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(DoubleDouble.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + n));
            final boolean jobSucceeded = job.waitForCompletion(true);
            tmpDataFile.delete();

            if (!jobSucceeded) {System.exit(1);}

                ++n;
            }
        }
    }
}
