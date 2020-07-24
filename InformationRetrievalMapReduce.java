package InformationRetrieval;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import net.iharder.base64.Base64.InputStream;

import java.io.File;

public class InformationRetrievalMapReduce extends Configured implements Tool {

	static int fileNumbers;

	public static class Task1Mapper extends Mapper<Object, Text, Text, InformationRetrievalObject> {

		private InformationRetrievalObject docFrequency = new InformationRetrievalObject();
		private Text term = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			List<String> stopwords = Files.readAllLines(Paths.get("/home/hduser/eclipse-workspace/InformationRetrieval/src/tr-stopword-list.txt"));
			//Scanner s = new Scanner(new File("/home/hduser/eclipse-workspace/InformationRetrieval/src/converted.txt"));
			//ArrayList<String> stopwords = new ArrayList<String>();
		//	while (s.hasNext()){
			//	stopwords.add(s.next());
		//	}
		//	s.close();
			int one = 1;
			String tweet = null;
			String valString = value.toString();
			String[] parts = valString.split("\t", 4);

			if (parts[0].equals("$PYTWEET$")) {

				tweet = parts[3];
			} else {
				tweet = valString;
			}
			// tweet = tweet.replaceAll("\\bhttp[^\\s]*"," ");
			tweet = tweet.replaceAll("[^a-zA-ZçğıöşüÇĞİÖŞÜ0-9]+"," ");
			tweet = tweet.toLowerCase();
			StringTokenizer itr = new StringTokenizer(tweet);
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			while (itr.hasMoreTokens()) {
				String word = itr.nextToken();
				if(word.toLowerCase().equals("para")) {
					int fff=0;
				}
				if (!stopwords.contains(word) && word.length() > 2) {
					term.set(word);
					docFrequency.set(fileName, one, word);
					context.write(term, docFrequency);

				}
			}
		}
	}

	public static class Task1Combiner
			extends Reducer<Text, InformationRetrievalObject, Text, InformationRetrievalObject> {

		public void reduce(Text key, Iterable<InformationRetrievalObject> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			String id = "";
			for (InformationRetrievalObject val : values) {
				count++;
				if (count == 1) {
					id = val.getDocumentID().toString();
				}
			}

			InformationRetrievalObject writable = new InformationRetrievalObject();
			writable.set(id, count, key.toString());
			context.write(key, writable);
		}
	}

	public static class Task1Reducer extends Reducer<Text, InformationRetrievalObject, Text, Text> {

		public void reduce(Text key, Iterable<InformationRetrievalObject> values, Context context)
				throws IOException, InterruptedException {

			HashMap<Text, IntWritable> map = new HashMap<Text, IntWritable>();
			for (InformationRetrievalObject val : values) {
				Text docID = new Text(val.getDocumentID());
				int freq = val.getFreq().get();
				if (map.get(docID) != null) {
					map.put(docID, new IntWritable(map.get(docID).get() + freq));
				} else {
					map.put(docID, new IntWritable(freq));
				}
			}

			Iterator<Entry<Text, IntWritable>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Text, IntWritable> pair = it.next();
				context.write(key, new Text(pair.getKey().toString() + "\t" + pair.getValue().toString()));
				it.remove(); // avoids a ConcurrentModificationException }
			}

		}
	}

	public static class Task2Mapper extends Mapper<Object, Text, Text, InformationRetrievalObject> {

		private InformationRetrievalObject termObject = new InformationRetrievalObject();
		private Text term = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String valString = value.toString();
			String[] parts = valString.split("\t");

			String documentID = parts[1];
			int freq = Integer.parseInt(parts[2]);
			String word = parts[0];

			term.set(documentID);
			termObject.set(documentID, freq, word);
			context.write(term, termObject);

		}

	}

	public static class Task2Reducer extends Reducer<Text, InformationRetrievalObject, Text, Text> {

		public void reduce(Text key, Iterable<InformationRetrievalObject> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			MarkableIterator<InformationRetrievalObject> mitr = new MarkableIterator<InformationRetrievalObject>(
					values.iterator());
			mitr.mark();
			InformationRetrievalObject temp;

			while (mitr.hasNext()) {
				temp = mitr.next();
				sum += temp.getFreq().get();
			}
			mitr.reset();

			while (mitr.hasNext()) {
				temp = mitr.next();
				String word = temp.getWord().toString();
				String freq = temp.getFreq().toString();

				context.write(new Text(word), new Text(key.toString() + "\t" + freq + "\t" + sum));
			}

		}
	}

	public static class Task3Mapper extends Mapper<Object, Text, Text, InformationRetrievalObject> {

		private InformationRetrievalObject termObject = new InformationRetrievalObject();
		private Text term = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String valString = value.toString();
			String[] parts = valString.split("\t");

			String documentID = parts[1];
			int freq = Integer.parseInt(parts[2]);
			String word = parts[0];
			int totalWordsOfDocID = Integer.parseInt(parts[3]);

			term.set(word);
			termObject.set2(documentID, freq, word, totalWordsOfDocID);
			context.write(term, termObject);

		}

	}

	public static class Task3Reducer extends Reducer<Text, InformationRetrievalObject, Text, Text> {
		// private Text term = new Text();

		public void reduce(Text key, Iterable<InformationRetrievalObject> values, Context context)
				throws IOException, InterruptedException {
			// private Text term = new Text();

			int sum = 0;
			MarkableIterator<InformationRetrievalObject> mitr = new MarkableIterator<InformationRetrievalObject>(
					values.iterator());
			mitr.mark();
			InformationRetrievalObject temp;

			while (mitr.hasNext()) {
				temp = mitr.next();
				sum += temp.getFreq().get();
			}
			mitr.reset();

			while (mitr.hasNext()) {
				temp = mitr.next();
				String word = temp.getWord().toString();
				String freq = temp.getFreq().toString();
				String docID = temp.getDocumentID().toString();
				int totalWordsOfDoc = temp.getTotalWordsOfDocID().get();

				context.write(new Text(word), new Text(docID + "\t" + freq + "\t" + totalWordsOfDoc + "\t" + sum));
			}
		}
	}

	public static class Task4Mapper extends Mapper<Object, Text, Text, InformationRetrievalObject> {

		private InformationRetrievalObject termObject = new InformationRetrievalObject();
		private Text term = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String valString = value.toString();
			String[] parts = valString.split("\t");

			String documentID = parts[1];
			int freq = Integer.parseInt(parts[2]);
			String word = parts[0];
			int totalWordsOfDocID = Integer.parseInt(parts[3]);
			int allThisWord = Integer.parseInt(parts[4]);

			term.set(word);
			termObject.set3(documentID, freq, word, totalWordsOfDocID, allThisWord);
			context.write(term, termObject);

		}

	}

	public static class Task4Reducer extends Reducer<Text, InformationRetrievalObject, Text, DoubleWritable> {
		// private Text term = new Text();
		DoubleWritable result = new DoubleWritable();


		public void reduce(Text key, Iterable<InformationRetrievalObject> values, Context context)
				throws IOException, InterruptedException {


			for (InformationRetrievalObject val : values) {

				//result = val.TFIDFCalculation(fileNumbers);
				String word = val.getWord().toString();
				String docID = val.getDocumentID().toString();
				String last = word + "\t" + docID;
				double x=val.TFIDFCalculation(fileNumbers);
				result.set(x);
				context.write(new Text(last), result);

			}

		}
	}

	

	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: InformationRetrievalMapReduce <in> <out>");
			System.exit(2);
		}
		String inputPath = args[0];
		String outputPath = args[1];
		String job1OutputPath = outputPath + "Job1";
		String job2OutputPath = outputPath + "Job2";
		String job3OutputPath = outputPath + "Job3";
		String job4OutputPath = outputPath + "Job4";
		File Files = new File(inputPath);
		//fileNumbers = Files.list().length;
		fileNumbers=1;

		Job job1 = Job.getInstance(getConf(), "TextOutInvertedIndexer");

		job1.setJarByClass(InformationRetrievalMapReduce.class);
		job1.setMapperClass(Task1Mapper.class);
		job1.setReducerClass(Task1Reducer.class);
		job1.setCombinerClass(Task1Combiner.class);
		// job1.setNumReduceTasks(2);
		job1.setOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(InformationRetrievalObject.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));

		Job job2 = Job.getInstance(getConf(), "log-analysis");
		job2.setJarByClass(InformationRetrievalMapReduce.class);
		job2.setMapperClass(Task2Mapper.class);
		job2.setReducerClass(Task2Reducer.class);
		// job2.setNumReduceTasks(3);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapOutputValueClass(InformationRetrievalObject.class);
		FileInputFormat.setInputPaths(job2, new Path(job1OutputPath + "/part*"));
		FileOutputFormat.setOutputPath(job2, new Path(job2OutputPath));

		Job job3 = Job.getInstance(getConf(), "log3-analysis");
		job3.setJarByClass(InformationRetrievalMapReduce.class);
		job3.setMapperClass(Task3Mapper.class);
		job3.setReducerClass(Task3Reducer.class);
		// job3.setNumReduceTasks(2);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapOutputValueClass(InformationRetrievalObject.class);
		FileInputFormat.setInputPaths(job3, new Path(job2OutputPath + "/part*"));
		FileOutputFormat.setOutputPath(job3, new Path(job3OutputPath));

		Job job4 = Job.getInstance(getConf(), "log4-analysis");
		job4.setJarByClass(InformationRetrievalMapReduce.class);
		job4.setMapperClass(Task4Mapper.class);
		job4.setReducerClass(Task4Reducer.class);
		// job4.setNumReduceTasks(3);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setMapOutputValueClass(InformationRetrievalObject.class);
		FileInputFormat.setInputPaths(job4, new Path(job3OutputPath + "/part*"));
		FileOutputFormat.setOutputPath(job4, new Path(job4OutputPath));

		ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
		ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
		ControlledJob controlledJob3 = new ControlledJob(job3.getConfiguration());
		ControlledJob controlledJob4 = new ControlledJob(job4.getConfiguration());
		controlledJob2.addDependingJob(controlledJob1);
		controlledJob3.addDependingJob(controlledJob2);
		controlledJob4.addDependingJob(controlledJob3);

		JobControl jobControl = new JobControl("JobControlDemoGroup");
		jobControl.addJob(controlledJob1);
		jobControl.addJob(controlledJob2);
		jobControl.addJob(controlledJob3);
		jobControl.addJob(controlledJob4);

		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();

		while (!jobControl.allFinished()) {
			Thread.sleep(500);
		}

		jobControl.stop();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//java.io.InputStream input = getClass().getResourceAsStream("/path/to/data.txt");

		
		
		String inputPath = args[0];
		String outputPath = args[1];
		String job1OutputPath = outputPath + "Job1";
		String job2OutputPath = outputPath + "Job2";
		String job3OutputPath = outputPath + "Job3";
		String job4OutputPath = outputPath + "Job4";
		//File Files = new File(inputPath);
		//fileNumbers = Files.list().length;
		fileNumbers=19;

		
		Job job1 = Job.getInstance(conf, "word count");
		job1.setJarByClass(InformationRetrievalMapReduce.class);
		job1.setMapperClass(Task1Mapper.class);
		job1.setCombinerClass(Task1Combiner.class);
		job1.setReducerClass(Task1Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(InformationRetrievalObject.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));
		
		Job job2 = Job.getInstance(conf, "log-analysis");
		job2.setJarByClass(InformationRetrievalMapReduce.class);
		job2.setMapperClass(Task2Mapper.class);
		job2.setReducerClass(Task2Reducer.class);
		// job2.setNumReduceTasks(3);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapOutputValueClass(InformationRetrievalObject.class);
		FileInputFormat.setInputPaths(job2, new Path(job1OutputPath + "/part*"));
		FileOutputFormat.setOutputPath(job2, new Path(job2OutputPath));
		
		Job job3 = Job.getInstance(conf, "log3-analysis");
		job3.setJarByClass(InformationRetrievalMapReduce.class);
		job3.setMapperClass(Task3Mapper.class);
		job3.setReducerClass(Task3Reducer.class);
		// job3.setNumReduceTasks(2);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapOutputValueClass(InformationRetrievalObject.class);
		FileInputFormat.setInputPaths(job3, new Path(job2OutputPath + "/part*"));
		FileOutputFormat.setOutputPath(job3, new Path(job3OutputPath));

		Job job4 = Job.getInstance(conf, "log4-analysis");
		job4.setJarByClass(InformationRetrievalMapReduce.class);
		job4.setMapperClass(Task4Mapper.class);
		job4.setReducerClass(Task4Reducer.class);
		// job4.setNumReduceTasks(3);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setMapOutputValueClass(InformationRetrievalObject.class);
		FileInputFormat.setInputPaths(job4, new Path(job3OutputPath + "/part*"));
		FileOutputFormat.setOutputPath(job4, new Path(job4OutputPath));

		
		ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
		ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
		ControlledJob controlledJob3 = new ControlledJob(job3.getConfiguration());
		ControlledJob controlledJob4 = new ControlledJob(job4.getConfiguration());
		controlledJob2.addDependingJob(controlledJob1);
		controlledJob3.addDependingJob(controlledJob2);
		controlledJob4.addDependingJob(controlledJob3);

		JobControl jobControl = new JobControl("JobControlDemoGroup");
		jobControl.addJob(controlledJob1);
		jobControl.addJob(controlledJob2);
		jobControl.addJob(controlledJob3);
		jobControl.addJob(controlledJob4);

		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();
		
		while (!jobControl.allFinished()){
			Thread.sleep(500);
		}
	
		jobControl.stop();
		
		//System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}