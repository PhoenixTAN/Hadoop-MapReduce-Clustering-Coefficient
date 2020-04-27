/*
Author: Ziqi Tan
*/
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriangleCount {

	/**
	 * Mapper1: Read all the edges. Emit <vertex, vertex>
	 */
	public static class EdgeReader extends Mapper<Object, Text, LongWritable, LongWritable> {

		private LongWritable vertex = new LongWritable();
		private LongWritable neighbor = new LongWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/**
			 * value: input graph as a string
			 */
			// System.out.println("Mapper1: ================================");
			StringTokenizer edgeIterator = new StringTokenizer(value.toString());
			while (edgeIterator.hasMoreTokens()) {
				vertex.set(Long.parseLong(edgeIterator.nextToken()));
				if (!edgeIterator.hasMoreTokens()) {
					throw new RuntimeException("Invalid edge in EdgeReader.");
				}
				neighbor.set(Long.parseLong(edgeIterator.nextToken()));
				context.write(vertex, neighbor);
			}
		}
	}

	/**
	 * Reducer1: 
	 * 		emit([v1, v2], key)
	 * 		where v1 and v2 are neighbors of key.
	 */
	public static class EdgeReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
		private Text pair = new Text(); 
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			// System.out.println("Reducer1: ================================");
			int size = 0;	// number of neighbors
			
			long[] neighbors = new long[4096];	// we need to perform a nested loop on the iterable values
			
			Iterator<LongWritable> neighborIterator = values.iterator();
			
			while (neighborIterator.hasNext()) {
				if( neighbors.length == size ) {
					// expand the capacity
					neighbors = Arrays.copyOf(neighbors, (int) (size*1.5));
				}
				
				long neighbor = neighborIterator.next().get();
				
				neighbors[size++] = neighbor;
				
				// emit actual edges
				String actualEdge = key.toString() + "," + Long.toString(neighbor);
				pair.set(actualEdge);
				context.write(pair, new Text("$"));		
			}
			
			// emit possible triangles
			for ( int i = 0; i < size; i++ ) {
				for ( int j = i + 1; j < size; j++ ) {
					String possibleEdge = Long.toString(neighbors[i]) + "," + Long.toString(neighbors[j]);
					pair.set(possibleEdge);
					context.write(pair, new Text(key.toString()));
				}
			}
		}
	}
	
	/**
	 * Mapper2: 
	 * 		Produce actual triangles.
	 * */
	public static class TriangleMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text pairKey = new Text();
		private Text outValue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// System.out.println("Mapper2: ================================");
			StringTokenizer lines = new StringTokenizer(value.toString());
			while ( lines.hasMoreTokens() ) {
				String pair = lines.nextToken().toString();
				if ( !lines.hasMoreTokens() ) {
					throw new RuntimeException("Invalid line in TriangleProducer.");
				}
				String symbol = lines.nextToken().toString();
				
				if ( symbol.contentEquals("$") ) {
					// System.out.println("Adding edge ...");
					pairKey.set(pair);
					outValue.set(symbol);
					context.write(pairKey, outValue);
				}
				else {
					// System.out.println("Possible edge ...");
					pairKey.set(pair);
					outValue.set(symbol);
					context.write(pairKey, outValue);
				}
			}
			
		}
	}
	
	/**
	 * Reducer2: 
	 * 		Triangle counter.
	 */
	public static class TriangleReducer extends Reducer<Text, Text, LongWritable, LongWritable> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// System.out.println("Reducer2: ================================");
			// System.out.println("Key: " + key.toString());
			long[] triangles = new long[4096];
			int size = 0;
			boolean isActualEdge = false;
			for ( Text symbol : values ) {
				// System.out.println("Symbol: " + symbol.toString() );
				if ( symbol.toString().contentEquals("$") ) {
					isActualEdge = true;
					// System.out.println("Actual Edge");
					continue;
				}
				
				long id = Long.parseLong(symbol.toString());
				
				if( triangles.length == size ) {
					// expand the capacity
					triangles = Arrays.copyOf(triangles, (int) (size*1.5));
				}
				
				triangles[size++] = id;
			}
			
			if ( isActualEdge ) {
				for ( int i = 0; i < size; i++ ) {
					context.write(new LongWritable(triangles[i]), new LongWritable(1));
				}
			}
		}
	}
	
	/**
	 * Mapper3: 
	 * 		Produce actual triangles.
	 * */
	public static class TriangleGetter extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		
		private LongWritable vertexKey = new LongWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// System.out.println("Mapper3=====================================");
			StringTokenizer vertexIterator = new StringTokenizer(value.toString());
			while ( vertexIterator.hasMoreTokens() ) {
				long vertex = Long.parseLong(vertexIterator.nextToken());
				vertexKey.set(vertex);
				if (!vertexIterator.hasMoreTokens()) {
					throw new RuntimeException("Invalid edge in EdgeReader.");
				}
				long num = Long.parseLong(vertexIterator.nextToken());
				// System.out.println("vertexKey: "  + vertex);
				context.write(vertexKey, new LongWritable(num));
			}
		}
		
	}
	
	/**
	 * Reducer3: 
	 * 		Triangle counter.
	 */
	public static class TriangleCounter extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			// System.out.println("Counter==========================");
			long count = 0;
			for ( LongWritable val : values ) {
				count += val.get();
			}
			// System.out.println("key: " + key.get() + "  count: " + count);
			context.write(new LongWritable(key.get()), new LongWritable(count));
		}
	}

	/**
	 * Main driver
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		// Job 1
		Job job1 = Job.getInstance(conf, "Triangle Count1");
		
		job1.setJarByClass(TriangleCount.class);
		job1.setMapperClass(EdgeReader.class);		
		job1.setReducerClass(EdgeReducer.class);
		
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(LongWritable.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path("input/bigGraph"));
		FileOutputFormat.setOutputPath(job1, new Path("reducer1outputBig"));
		
		// Job 2
		Job job2 = Job.getInstance(conf, "Triangle Count2");
		job2.setJarByClass(TriangleCount.class);
		job2.setMapperClass(TriangleMapper.class);
		job2.setReducerClass(TriangleReducer.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path("reducer1outputBig"));
		FileOutputFormat.setOutputPath(job2, new Path("trianglesBig"));
		
		// Job 3
		Job job3 = Job.getInstance(conf, "Triangle Count3");
		job3.setJarByClass(TriangleCount.class);
		job3.setMapperClass(TriangleGetter.class);
		job3.setReducerClass(TriangleCounter.class);

		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(LongWritable.class);

		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job3, new Path("trianglesBig"));
		FileOutputFormat.setOutputPath(job3, new Path("outputBig"));
		
		int ret = job1.waitForCompletion(true) ? 0 : 1;
		
		if ( ret == 0 ) {
			ret = job2.waitForCompletion(true) ? 0 : 1;
		}
		
		if ( ret == 0 ) {
			ret = job3.waitForCompletion(true) ? 0 : 1;
		}
		
		System.exit(ret);
	}
}
