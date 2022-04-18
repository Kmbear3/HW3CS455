import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.*;
import java.util.Collections;
import java.text.SimpleDateFormat;




public class Question5 {
	public static class CountyWeekMapperQ5 extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            Long time = Long.parseLong(line[2]);
            Integer aqi = Integer.parseInt(line[1]);
            String county = line[0];

            SimpleDateFormat sdf = new SimpleDateFormat("w");
            String weekNum = sdf.format(time);

            sdf = new SimpleDateFormat("yyyy");
            String year = sdf.format(time);

            Text outKey = new Text(county + "-" + weekNum + "-" + year);
            IntWritable outVal = new IntWritable(aqi);
            context.write(outKey, outVal);
        }
	
    }

    public static class CountyWeekAverageReducerQ5 extends Reducer<Text, IntWritable, Text, DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            Integer sum = 0;
            Integer count = 0;
            for(IntWritable x : values){
                Integer aqi = x.get();
                sum += aqi;
                ++count;
            }

            Double avg = (double) sum / count;
            context.write(new Text(key), new DoubleWritable(avg));
        }

    }

    

	public static void main(String[] args) throws Exception {
        Path inputFile = new Path(args[1]);
        Path countyWeekAvgPath = new Path(args[2]);
        Path finalOutput = new Path(args[3]);

		Configuration conf = new Configuration(); 
		Job job = Job.getInstance(conf, "Question5-Job1"); 
		job.setJarByClass(Question5.class); 
		job.setMapperClass(Question5.CountyWeekMapperQ5.class); 
		job.setReducerClass(Question5.CountyWeekAverageReducerQ5.class); 
		job.setNumReduceTasks(1); 
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);  
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, countyWeekAvgPath);
		job.waitForCompletion(true);

        Configuration confJob2 = new Configuration(); 
		Job job2 = Job.getInstance(confJob2, "Question5-Job2"); 
		job2.setJarByClass(Question5Job2.class); 
		job2.setMapperClass(Question5Job2.CountyAvgYearMapperQ5.class); 
		job2.setReducerClass(Question5Job2.CountyLargestChangeReducerQ5.class); 
		job2.setNumReduceTasks(1); 
		job2.setMapOutputKeyClass(Text.class); 
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);  
		job2.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job2, countyWeekAvgPath);
		FileOutputFormat.setOutputPath(job2, finalOutput);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

}