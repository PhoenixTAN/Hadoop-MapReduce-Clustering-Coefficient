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

public class TriangleCounterImproved {

	/**
	 * Mapper1 and Reducer1 calculate the degree of the node in the first column.
	 * Let v1 be a node in the first column, v2 be a node in the second column. The
	 * output of Reducer1 will be <v2, v1, d1> where d1 is the degree of v1.
	 */

	/**
	 * Mapper1: input <v1,v2> emit <v1,v2>
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
	 * Reducer1: input: <v1, [neighbors of v1]> emit(neighbor's id, "v1,degree1")
	 */
	public static class Degree1Reducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
		private LongWritable keyOut = new LongWritable();
		private Text node = new Text();

		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			// System.out.println("Reducer1: ================================");
			int size = 0; // number of neighbors

			long[] neighbors = new long[4096]; // we need to perform a nested loop on the iterable values

			Iterator<LongWritable> neighborIterator = values.iterator();

			while (neighborIterator.hasNext()) {
				if (neighbors.length == size) {
					// expand the capacity
					neighbors = Arrays.copyOf(neighbors, (int) (size * 1.5));
				}
				long neighbor = neighborIterator.next().get();
				neighbors[size++] = neighbor;
			}

			for (int i = 0; i < size; i++) {
				long neighborId = neighbors[i];
				keyOut.set(neighborId);
				node.set(key.toString() + "," + Integer.toString(size));
				context.write(keyOut, node);
			}

		}
	}

	/**
	 * Mapper2 and Reducer2 do the same thing as Mapper1 and Reducer1 does,
	 * basically. Mapper2 and Reducer2 calculate the degree of the node in the first
	 * column. The input of Mapper2 is <"v2" "v1,d1">. The output of Reducer2 is
	 * <"v2,d2" "v1,d1">.
	 */
	public static class EdgeReader2 extends Mapper<Object, Text, LongWritable, Text> {

		private LongWritable vertex = new LongWritable();
		private Text node = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer edgeIterator = new StringTokenizer(value.toString());
			while (edgeIterator.hasMoreTokens()) {
				vertex.set(Long.parseLong(edgeIterator.nextToken()));
				if (!edgeIterator.hasMoreTokens()) {
					throw new RuntimeException("Invalid edge in EdgeReader.");
				}
				node.set(edgeIterator.nextToken().toString());
				context.write(vertex, node);
			}
		}
	}

	public static class Degree2Reducer extends Reducer<LongWritable, Text, Text, Text> {
		private Text node1 = new Text();
		private Text node2 = new Text();

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String[] neighbors = new String[4096];
			int size = 0;
			for (Text text : values) {
				if (neighbors.length == size) {
					// expand the capacity
					neighbors = Arrays.copyOf(neighbors, (int) (size * 1.5));
				}
				String neighbor = text.toString();
				neighbors[size++] = new String(neighbor);
			}

			node1.set(key.toString() + "," + Integer.toString(size));
			///////////////
			long id1 = Long.parseLong(key.toString());
			for (int i = 0; i < size; i++) {
				// node2.set(neighbors[i]);
				String[] neighbor = neighbors[i].split(",");
				long id2 = Long.parseLong(neighbor[0]);
				if (id1 < id2) {
					node2.set(neighbors[i]);
					context.write(node1, node2);
				}

			}
		}
	}

	/**
	 * Mapper3 and Reducer3 are the beginning of the improved algorithm. The input
	 * of Mapper3 is <"v1,d1" "v2,d2">.
	 * 
	 */
	public static class EdgeReader3 extends Mapper<Object, Text, LongWritable, LongWritable> {

		private LongWritable vertex = new LongWritable();
		private LongWritable neighbor = new LongWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer edgeIterator = new StringTokenizer(value.toString());
			while (edgeIterator.hasMoreTokens()) {
				// String token1 = edgeIterator.nextToken().toString();
				String[] node1 = edgeIterator.nextToken().toString().split(",");
				long id1 = Long.parseLong(node1[0]);
				long degree1 = Long.parseLong(node1[1]);

				if (!edgeIterator.hasMoreTokens()) {
					throw new RuntimeException("Invalid edge in EdgeReader.");
				}
				String[] node2 = edgeIterator.nextToken().toString().split(",");
				long id2 = Long.parseLong(node2[0]);
				long degree2 = Long.parseLong(node2[1]);

				if (degree1 > degree2) {
					vertex.set(id2);
					neighbor.set(id1);
					context.write(vertex, neighbor);
				} else {
					vertex.set(id1);
					neighbor.set(id2);
					context.write(vertex, neighbor);
				}
			}
		}
	}

	public static class EdgeReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
		private Text pair = new Text();

		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			int size = 0; // number of neighbors

			long[] neighbors = new long[4096]; // we need to perform a nested loop on the iterable values

			Iterator<LongWritable> neighborIterator = values.iterator();

			while (neighborIterator.hasNext()) {
				if (neighbors.length == size) {
					// expand the capacity
					neighbors = Arrays.copyOf(neighbors, (int) (size * 1.5));
				}

				long neighbor = neighborIterator.next().get();

				neighbors[size++] = neighbor;

				// emit actual edges
				/*
				 * String actualEdge = key.toString() + "," + Long.toString(neighbor);
				 * pair.set(actualEdge); context.write(pair, new Text("$"));
				 */
			}

			// emit possible triangles
			for (int i = 0; i < size; i++) {
				for (int j = i + 1; j < size; j++) {
					String possibleEdge = Long.toString(neighbors[i]) + "," + Long.toString(neighbors[j]);
					pair.set(possibleEdge);
					// context.write(pair, new Text(key.toString()));
					context.write(new Text(key.toString()), pair);
				}
			}
		}
	}

	/**
	 * Mapper4: Produce actual triangles.
	 */

	public static class TriangleMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text pairKey = new Text();
		private Text outValue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { //
			// System.out.println("Mapper2: ================================");
			// StringTokenizer lines = new StringTokenizer(value.toString());
			/*while (lines.hasMoreTokens()) {
				String pair = lines.nextToken().toString();
				if (!lines.hasMoreTokens()) {
					throw new RuntimeException("Invalid line in TriangleProducer.");
				}
				String symbol = lines.nextToken().toString();

				if (symbol.contentEquals("$")) { // System.out.println("Adding edge ...");
					pairKey.set(pair);
					outValue.set(symbol);
					context.write(pairKey, outValue);
				} else { // System.out.println("Possible edge ..."); pairKey.set(pair);
					outValue.set(symbol);
					context.write(pairKey, outValue);
				}
			}*/
			
			// input type 1
			/*
				0	1,2
				1	2,3
				4	5,3
			*/
			// input type 2
			/*
			 *  0	1
			 *  1   0
			 */
			
			String data = value.toString();
			String[] lines = data.split("\n");
			for( String line : lines ) {
				int index = line.indexOf(',');
				if( index != -1 ) {
					// input type 1
					String[] vPair = line.split("\t");
					String v = vPair[0];
					String pair = vPair[1];
					// System.out.println("type 1 --- v: " + v + "pair: " + pair);
					pairKey.set(pair);
					outValue.set(v);
					context.write(pairKey, outValue);
				}
				else {
					// input type 2
					String[] edge = line.split("\t");
					String v1 = edge[0];
					String v2 = edge[1];
					// System.out.println("type 1 --- v1: " + v1 + "v2: " + v2);
					String pair = v1 + "," + v2;
					pairKey.set(pair);
					outValue.set("$");
					context.write(pairKey, outValue);
				}
			}
			
			
		}
	}
	  
	 /**
		 * Reducer4: Triangle counter.
		 */

	public static class TriangleReducer extends Reducer<Text, Text, LongWritable, LongWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// System.out.println("Reducer2: ================================"); //
			System.out.println("Key: " + key.toString());
			long[] triangles = new long[4096];
			int size = 0;
			boolean isActualEdge = false;
			for (Text symbol : values) { // System.out.println("Symbol: " + symbol.toString() );
				if (symbol.toString().contentEquals("$")) {
					isActualEdge = true; //
					// System.out.println("Actual Edge");
					continue;
				}

				long id = Long.parseLong(symbol.toString());

				if (triangles.length == size) { // expand the capacity
					triangles = Arrays.copyOf(triangles, (int) (size * 1.5));
				}

				triangles[size++] = id;
			}

			if (isActualEdge) {
				for (int i = 0; i < size; i++) { 
					context.write(new LongWritable(triangles[i]), new LongWritable(1));
				}
			}
		}
	}

	 /**
		 * Mapper5: Produce actual triangles.
		 */

	/*
	 * 
	 * 
	 * public static class TriangleGetter extends Mapper<LongWritable, Text,
	 * LongWritable, LongWritable> {
	 * 
	 * private LongWritable vertexKey = new LongWritable();
	 * 
	 * public void map(LongWritable key, Text value, Context context) throws
	 * IOException, InterruptedException { //
	 * System.out.println("Mapper3=====================================");
	 * StringTokenizer vertexIterator = new StringTokenizer(value.toString()); while
	 * ( vertexIterator.hasMoreTokens() ) { long vertex =
	 * Long.parseLong(vertexIterator.nextToken()); vertexKey.set(vertex); if
	 * (!vertexIterator.hasMoreTokens()) { throw new
	 * RuntimeException("Invalid edge in EdgeReader."); } long num =
	 * Long.parseLong(vertexIterator.nextToken()); //
	 * System.out.println("vertexKey: " + vertex); context.write(vertexKey, new
	 * LongWritable(num)); } } }
	 * 
	 */
	/**
	* Reducer5: Triangle counter.
	*/
	/*
	public static class TriangleCounter extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException { //
			System.out.println("Counter==========================");
			long count = 0;
			for (LongWritable val : values) {
				count += val.get();
			} //
			System.out.println("key: " + key.get() + "  count: " + count);
			context.write(new LongWritable(key.get()), new LongWritable(count));
		}
	}
	 */

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// pass parameter in the console
		int data = Integer.parseInt(args[0]);
		/**
		 * 0: use a tiny graph. 1: small graph. 2: big graph.
		 */

		String DATADIR_PREFIX = "dataset";
		String OUTPUT_PREFIX = "output";
		String dataSetDir = "/test";
		String reducer1output = "/testReducer1output";
		String reducer2output = "/testReducer2output";
		String reducer3output = "/testReducer3output";
		String reducer4output = "/testReducer4output";
		String reducer5output = "/testReducer5output";
		switch (data) {
		case 0:
			dataSetDir = "/test";
			reducer1output = "/testReducer1output";
			reducer2output = "/testReducer2output";
			reducer3output = "/testReducer3output";
			reducer4output = "/testReducer4output";
			reducer5output = "/testReducer5output";
			System.out.println("Using test data set.");
			break;
		case 1:
			dataSetDir = "/small-graph";
			reducer1output = "/smallGraphReducer1output";
			reducer2output = "/smallGraphReducer2output";
			reducer3output = "/smallGraphReducer3output";
			reducer4output = "/smallGraphReducer4output";
			reducer5output = "/smallGraphReducer5output";
			System.out.println("Using small graph.");
			break;
		case 2:
			dataSetDir = "/big-graph";
			reducer1output = "/bigGraphReducer1output";
			reducer2output = "/bigGraphReducer2output";
			reducer3output = "/bigGraphReducer3output";
			reducer4output = "/bigGraphReducer4output";
			reducer5output = "/bigGraphReducer5output";
			System.out.println("Using big graph.");
			break;
		default:
			dataSetDir = "/test";
			reducer1output = "/testReducer1output";
			reducer2output = "/testReducer2output";
			reducer3output = "/testReducer3output";
			reducer4output = "/testReducer4output";
			reducer5output = "/testReducer5output";
			System.out.println("Using test data set.");
			break;
		}

		Configuration conf = new Configuration();

		// Job 1
		Job job1 = Job.getInstance(conf, "Triangle Count1");

		job1.setJarByClass(TriangleCounterImproved.class);
		job1.setMapperClass(EdgeReader.class);
		job1.setReducerClass(Degree1Reducer.class);

		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(DATADIR_PREFIX + dataSetDir));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PREFIX + reducer1output));

		// Job 2
		Job job2 = Job.getInstance(conf, "Triangle Count2");
		job2.setJarByClass(TriangleCounterImproved.class);
		job2.setMapperClass(EdgeReader2.class);
		job2.setReducerClass(Degree2Reducer.class);

		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(OUTPUT_PREFIX + reducer1output));
		FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PREFIX + reducer2output));

		// Job 3
		Job job3 = Job.getInstance(conf, "Triangle Count3");
		job3.setJarByClass(TriangleCounterImproved.class);
		job3.setMapperClass(EdgeReader3.class);
		job3.setReducerClass(EdgeReducer.class);

		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(LongWritable.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, new Path(OUTPUT_PREFIX + reducer2output));
		FileOutputFormat.setOutputPath(job3, new Path(OUTPUT_PREFIX + reducer3output));

		
		  // Job 4 
		Job job4 = Job.getInstance(conf, "Triangle Count4");
		job4.setJarByClass(TriangleCounterImproved.class);
		job4.setMapperClass(TriangleMapper.class);
		job4.setReducerClass(TriangleReducer.class);

		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);

		job4.setOutputKeyClass(LongWritable.class);
		job4.setOutputValueClass(LongWritable.class);

		// FileInputFormat.addInputPath(job4, new Path(OUTPUT_PREFIX +reducer3output));
		// FileInputFormat.setInputPaths(job4, new Path(OUTPUT_PREFIX + reducer3output + "," + DATADIR_PREFIX + dataSetDir));
		FileInputFormat.setInputPaths(job4, new String(OUTPUT_PREFIX + reducer3output + "," + DATADIR_PREFIX + dataSetDir));
		FileOutputFormat.setOutputPath(job4, new Path(OUTPUT_PREFIX + reducer4output));
		  /*
		  // Job 5 Job job5 = Job.getInstance(conf, "Triangle Count5");
		  job5.setJarByClass(TriangleCounterImproved.class);
		  job5.setMapperClass(TriangleGetter.class);
		  job5.setReducerClass(TriangleCounter.class);
		  
		  job5.setMapOutputKeyClass(LongWritable.class);
		  job5.setMapOutputValueClass(LongWritable.class);
		  
		  job5.setOutputKeyClass(LongWritable.class);
		  job5.setOutputValueClass(LongWritable.class);
		  
		  FileInputFormat.addInputPath(job5, new Path(OUTPUT_PREFIX + reducer4output));
		  FileOutputFormat.setOutputPath(job5, new Path(OUTPUT_PREFIX +
		  reducer5output));
		 */

		int ret = job1.waitForCompletion(true) ? 0 : 1;

		if (ret == 0) {
			ret = job2.waitForCompletion(true) ? 0 : 1;
		}

		if (ret == 0) {
			ret = job3.waitForCompletion(true) ? 0 : 1;
		}

		
		if (ret == 0) { ret = job4.waitForCompletion(true) ? 0 : 1; }
		  
		  // if (ret == 0) { ret = job5.waitForCompletion(true) ? 0 : 1; }
		 

		System.exit(ret);

	}

}
