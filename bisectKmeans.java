//BISECTING K MEANS ALGORITHM USING HADOOP MAP REDUCE


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import org.apache.hadoop.fs.FileSystem;


public class bisectKmeans {

	static int numCluster = 0;
	static int noAttr=0;
	static int iter=0;
	static Map<Integer,Double[]>centroid = new HashMap<Integer,Double[]>(); 
	static Map<Integer,Double[]>previous = null; 
	static Map<Integer,Integer> rowToCenter = new TreeMap<Integer,Integer>(); 

	public static class kMapper extends Mapper<Object, Text, IntWritable, Text>{

		private Text point = new Text();
		private int row_no = 0;
		public static Double calculateDistance(Double[] point1, Double[] point2) {
			Double absDst = 0.0;
			Double dist =0.0;
			for (int i = 0; i < point1.length; i++) {
				dist = Math.abs(point1[i] - point2[i]);
				absDst += dist * dist;
			}
			return absDst;
		}


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();


			String[] lineArray = line.split(" ");
			int row_no = Integer.parseInt(lineArray[0])-1;
			StringBuilder pointData = new StringBuilder();
			noAttr = lineArray.length - 2;
			Double minDist = Double.MAX_VALUE;
			int nearest = 0;
			Double[] myPoint = new Double[noAttr];
			Double distFromCenter = 0.0;

			for (int i = 0; i < myPoint.length; i++) 
			{
				myPoint[i] = Double.parseDouble(lineArray[i + 2]);
				pointData.append(lineArray[i + 2] + " ");
			}

			for (int center: centroid.keySet()) 
			{
				distFromCenter = calculateDistance(centroid.get(center), myPoint);  //get distance of each row from all centroids
				if (distFromCenter < minDist) 
				{
					minDist = distFromCenter;
					nearest = center;
				}
			}

			IntWritable nearestCentroid = new IntWritable(nearest);
			point.set(line);
			context.write(nearestCentroid, point
	);


		}
	}

	public static class kReducer extends Reducer<IntWritable, Text,IntWritable,Text> {

		private Text newCentroidMean = new Text();

		public void reduce(IntWritable clusterId, Iterable<Text> rows, Context context)
				throws IOException, InterruptedException 
		{
			Double count = 0.0;
			Double[] mean = new Double[noAttr];

			Iterator<Text> it =rows.iterator();
			while (it.hasNext()) 
			{
				Text line= it.next();

				String pointData = line.toString();
				String[] pointDataArray = pointData.split(" ");
				rowToCenter.put(Integer.parseInt(pointDataArray[0])-1,clusterId.get());

				for (int i = 0; i < noAttr; i++) 
				{
					mean[i] = (mean[i]==null?0.0:mean[i])+Double.parseDouble(pointDataArray[i+2]);
				}
				count++;
			}

			StringBuilder meanString = new StringBuilder();

			for (int i = 0; i < noAttr; i++) 
			{
				mean[i] = mean[i] / count;
				meanString.append(mean[i] + " ");
			}

			centroid.put(clusterId.get(),mean);
		}
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		String OUT ="/output/";
		String input = "/bisectkmeans/";
		String initialFolder = "/bisectkmeans/";
		String DATASET = "dataset.txt";
		String CENTROID ="center.txt";
		String output = OUT + System.nanoTime();

		int finalClusters = 0 // number of clusters
		while(finalClusters < 8){
			boolean randomCentroid = false;
			int preDefCentroids=2; // inner K = 2 always
			int cluster_number=0;
			PriorityQueue<Integer> centroidQueue = new PriorityQueue<Integer>();
			String line ="";

			if(randomCentroid)
			{

				cluster_number=preDefCentroids;
				for(int i =0; i<cluster_number;i++)
					centroidQueue.offer(i);
			}
			else
			{

				Path centroid_file = new Path(initialFolder+CENTROID);
				FileSystem f = FileSystem.get(new Configuration());
				DataInputStream centroidStream = new DataInputStream(f.open(centroid_file));
				BufferedReader buff = new BufferedReader(new InputStreamReader(centroidStream));
				int x =2 ;

				while ((line = buff.readLine())!=null && x>0) 
				{
					centroidQueue.offer(Integer.parseInt(line));
					x -- ;
				}

				buff.close();
				cluster_number = centroidQueue.size();

			}


			Path input_file = new Path(input+DATASET);
			FileSystem fs = FileSystem.get(new Configuration());
			DataInputStream stream = new DataInputStream(fs.open(input_file));
			BufferedReader br = new BufferedReader(new InputStreamReader(stream));

			int lineNum =0; 
			int clusterNo = 0;

			while((!centroidQueue.isEmpty())  && ((line=br.readLine())!=null))
			{
				if(lineNum != centroidQueue.peek())
				{
					lineNum++;
					continue;
				}
				centroidQueue.poll();
				String[] sp = line.split(" ");
				Double[] temp = new Double[sp.length-2];
				for(int j=0;j<sp.length-2;j++)
				{
					temp[j]=Double.parseDouble(sp[j+2]);
				}
				centroid.put(clusterNo++,temp);
				lineNum++;

			}	
			br.close();

			while(true)
			{


				Configuration conf = new Configuration();
				Job job = Job.getInstance(conf, "kmeans");

				job.setJarByClass(bisectKmeans.class);
				job.setMapperClass(kMapper.class);
				job.setReducerClass(kReducer.class);

				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job, new Path(input+DATASET));
				FileOutputFormat.setOutputPath(job, new Path(output + String.valueOf(iter)));
				job.waitForCompletion(true);
				if(checkConvergence(centroid,previous))
					break;
				previous = new HashMap<Integer, Double[]>();
				copyCentroid(previous, centroid);            
				iter++;

			}
			finalClusters += 1 ;
		}	
		long endTime = System.currentTimeMillis();
		System.out.println("Time: "+((endTime-startTime)/1000)+" seconds");
	}

	public static void copyCentroid(Map<Integer,Double[]> previous, Map<Integer,Double[]> centroid)
	{
		for(Integer center: centroid.keySet())
		{
			previous.put(center, centroid.get(center));
		}
	}

	public static boolean checkConvergence(Map<Integer,Double[]> centroid, Map<Integer,Double[]> previous)
	{
		if(previous==null||centroid==null)
			return false;

		for(Integer temp : centroid.keySet())
		{
			for(int i = 0; i<centroid.get(temp).length;i++){

				if (!centroid.get(temp)[i].equals(previous.get(temp)[i])){
					return false;
				}
			}
		}

		return true;	
	}
}