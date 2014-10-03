import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class WordCount extends Configured implements Tool {

	public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		private Text year_word = new Text();
		private DoubleWritable volumes = new DoubleWritable();

		public void configure(JobConf job) {
		}

		public static boolean isNumeric(String s){
			try{Integer.parseInt(s);}
			catch(NumberFormatException e){return false;}
			return true;
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();

		    String thisWord = "";
		    String thisYear = "";
		    double thisVolume = 0;
		    
		    String[] strList = line.split("\t");
		    thisWord = strList[0].toLowerCase();
		    thisYear = strList[1];
		    thisVolume = Double.parseDouble(strList[3]);

		    if(isNumeric(thisYear)){
		    	if(thisWord.contains("nu")){
		    		year_word.set(thisYear + " nu");
		    		volumes.set(thisVolume);
		    		output.collect(year_word,volumes);
		    	}
		    	if(thisWord.contains("die")){
		    		year_word.set(thisYear + " die");
		    		volumes.set(thisVolume);
		    		output.collect(year_word,volumes);
		    	}
		    	if(thisWord.contains("kla")){
		    		year_word.set(thisYear + " kla");
		    		volumes.set(thisVolume);
		    		output.collect(year_word,volumes);
		    	}
		    }
		}
	}

	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		private Text year_word = new Text();
		private DoubleWritable volumes = new DoubleWritable();

		public void configure(JobConf job) {
		}

		public static boolean isNumeric(String s){
			try{Integer.parseInt(s);}
			catch(NumberFormatException e){return false;}
			return true;
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();

		    String thisWord = "";
		    String thisYear = "";
		    double thisVolume = 0;
		    
		    String[] strList = line.split("\\s+|\\t+");
		    thisWord = (strList[0] + " " + strList[1]).toLowerCase();
		    thisYear = strList[2];
		    thisVolume = Double.parseDouble(strList[4]);

		    if(isNumeric(thisYear)){
		    	if(thisWord.contains("nu")){
		    		year_word.set(thisYear + " nu");
		    		volumes.set(thisVolume);
		    		output.collect(year_word,volumes);
		    	}
		    	else if(thisWord.contains("die")){
		    		year_word.set(thisYear + " die");
		    		volumes.set(thisVolume);
		    		output.collect(year_word,volumes);
		    	}
		    	else if(thisWord.contains("kla")){
		    		year_word.set(thisYear + " kla");
		    		volumes.set(thisVolume);
		    		output.collect(year_word,volumes);
		    	}
		    }
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			double sum = 0;
			int count = 0;
			double avg = 0;
			while(values.hasNext()){
				sum = sum + values.next().get();
				count = count + 1;
			}
			avg = sum / count;
			output.collect(key, new DoubleWritable(avg));
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), WordCount.class);
		conf.setJobName("google1");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		MultipleInputs.addInputPath(conf, new Path(args[0]),TextInputFormat.class,Map1.class);
		MultipleInputs.addInputPath(conf, new Path(args[1]),TextInputFormat.class,Map2.class);
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		JobClient.runJob(conf);
		return 0;
    }

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
    }
}
