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




public class Question6 {
	public static class CountyWeekMapperQ6 extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            Long time = Long.parseLong(line[2]);
            Integer aqi = Integer.parseInt(line[1]);
            String county = line[0];

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
            String year = sdf.format(time);

            sdf = new SimpleDateFormat("MMM");
            String month = sdf.format(time);

            Text outKey = new Text(county + "-" + month);
            Text outVal = new Text(aqi + "-" + year);
            context.write(outKey, outVal);
        }
	
    }

    public static class CountyWeekAverageReducerQ6 extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Integer sum = 0;
            Integer count = 0;
            Integer sum2020 = 0;
            Integer count2020 = 0;

            ArrayList<String[]> vals = new ArrayList<>();
            for(Text x : values){
                String[] aqiYear = x.toString().split("-");
                vals.add(aqiYear);

                Integer aqi = Integer.parseInt(aqiYear[0]);
                String year = aqiYear[1];
                if(year.equals("2021")){
                    continue;
                }
                else if(!year.equals("2020")){
                    sum += aqi;
                    ++count;
                }else{ //2020 data
                    sum2020 += aqi;
                    ++count2020;
                }
            }
            Double avgAQI = (double) sum / count;
            Double avgAQI2020 = (double) sum2020 / count2020;

            Double diffSum = 0.0;
            Integer count2 = 0;
            for(String[] aqiYear: vals){
                Integer aqi = Integer.parseInt(aqiYear[0]);
                String year = aqiYear[1];

                Double diff2 = Math.pow((aqi - avgAQI),2);
                if(!year.equals("2020")){
                    diffSum += diff2;
                    ++count2;
                }
            }

            Double std_dev = Math.sqrt(diffSum / count);

            if(Math.abs(avgAQI2020 - avgAQI) > std_dev){
                 context.write(new Text(key.toString() + "-2020"), new Text("2020avg=" + avgAQI2020 + "||pastAvg=" + avgAQI + "||std_dev=" + std_dev));
            }  
        }

    }

    

	public static void main(String[] args) throws Exception {
        Path inputFile = new Path(args[1]);
        Path finalOutput = new Path(args[2]);

		Configuration conf = new Configuration(); 
		Job job = Job.getInstance(conf, "Question6-Job1"); 
		job.setJarByClass(Question6.class); 
		job.setMapperClass(Question6.CountyWeekMapperQ6.class); 
		job.setReducerClass(Question6.CountyWeekAverageReducerQ6.class); 
		job.setNumReduceTasks(3); 
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);  
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, finalOutput);
		job.waitForCompletion(true);

	}

}