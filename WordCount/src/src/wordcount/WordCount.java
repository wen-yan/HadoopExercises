package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class WordCount extends Configured implements Tool
{
	// Custom Counters
	public static enum Counters
	{
		MapCounter,
		CombineCounter,
		ReduceCounter,
	}

	public static final Logger LOG = Logger.getGlobal();

	public static void main(String[] args) throws Exception
	{
		LOG.info("main() started");

		int res = ToolRunner.run(new WordCount(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if(args.length < 2)
		{
			System.err.println("Usage: wordcount <in> <out>");

			for(String arg : args)
			{
				System.err.println(arg);
			}
			System.exit(2);
		}

		Job job = Job.getInstance(getConf(), "wordcount");

		for(int i = 0; i < args.length; i += 1)
		{
			if("-skip".equals(args[i]))
			{
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
				i += 1;
				job.addCacheFile(new Path(args[i]).toUri());

				// this demonstrates logging
				LOG.info("Added file to the distributed cache: " + args[i]);
			}
		}

		job.setJarByClass(this.getClass());

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set reduce task count will cause same count output files !!!
		//job.setNumReduceTasks(2);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one             = new IntWritable(1);
		private static final Pattern     WORD_BOUNDARY   = Pattern.compile("\\s*\\b\\s*");
		private              boolean     _caseSensitive  = false;
		private final        Set<String> _patternsToSkip = new HashSet<String>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);

			Configuration config = context.getConfiguration();
			_caseSensitive = config.getBoolean("wordcount.case.sensitive", false);

			if(config.getBoolean("wordcount.skip.patterns", false))
			{
				URI[] localPaths = context.getCacheFiles();
				this.ParseSkipFile(localPaths[0]);
			}
		}

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException
		{
			String line = lineText.toString();
			LOG.info("map() + " + line);
			System.out.println("map() + " + line);

			context.getCounter(Counters.MapCounter).increment(1);

			if(!_caseSensitive)
				line = line.toLowerCase();

			for(String word : WORD_BOUNDARY.split(line))
			{
				if(word.isEmpty() || _patternsToSkip.contains(word))
				{
					continue;
				}

				Text currentWord = new Text(word);
				context.write(currentWord, one);
			}
		}

		private void ParseSkipFile(URI patternsURI)
		{
			LOG.info("Added file to the distributed cache: " + patternsURI);
			try
			{
				BufferedReader fis = new BufferedReader(
					new FileReader(new File(patternsURI.getPath()).getName()));

				String pattern;
				while((pattern = fis.readLine()) != null)
				{
					_patternsToSkip.add(pattern);
				}
			}
			catch(IOException ioe)
			{
				System.err.println(
					"Caught exception while parsing the cached file '" + patternsURI + "' : " +
						StringUtils.stringifyException(ioe));
			}
		}
	}

	// Combiner works on locally before transmission, so can save bandwidth and time.
	// For real application, can use Reducer as Combiner, but in this sample, use
	// different classes to calculate combine counter.
	public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException
		{
			context.getCounter(Counters.CombineCounter).increment(1);

			int sum = 0;
			int length = 0;
			for(IntWritable count : counts)
			{
				sum += count.get();
				length++;
			}

			LOG.info(
				"reduce() + " + word.toString() + ", counts length = " + Integer.toString(length));
			System.out.println(
				"reduce() + " + word.toString() + ", counts length = " + Integer.toString(length));
			context.write(word, new IntWritable(sum));
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException
		{
			context.getCounter(Counters.ReduceCounter).increment(1);

			int sum = 0;
			int length = 0;
			for(IntWritable count : counts)
			{
				sum += count.get();
				length++;
			}

			LOG.info(
				"reduce() + " + word.toString() + ", counts length = " + Integer.toString(length));
			System.out.println(
				"reduce() + " + word.toString() + ", counts length = " + Integer.toString(length));
			context.write(word, new IntWritable(sum));
		}
	}
}
