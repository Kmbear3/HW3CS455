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

public class Question5Job2 {
	public static class CountyAvgYearMapperQ5 extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("-");
            String county = vals[0];
            String week = vals[1];
            String[] yearAQI = vals[2].split("\t");
            String year  = yearAQI[0];

            Double avgAQI = Double.parseDouble(yearAQI[1]);

            context.write(new Text(county), new Text(week + "-" + year + "-" + avgAQI));
        }
	
    }

    public static class CountyLargestChangeReducerQ5 extends Reducer<Text, Text, Text, DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Map<String, Double> weekToAQI = new HashMap<>();
            for(Text x : values){
                String[] vals = x.toString().split("-");
                Integer week = Integer.parseInt(vals[0]);
                Integer year = Integer.parseInt(vals[1]);
                Double avgAQI = Double.parseDouble(vals[2]);

                String mapKey = week + "-" + year;
                weekToAQI.put(mapKey, avgAQI);
            }

            List<Double> changes = new ArrayList<>();
            for(String weekYear : weekToAQI.keySet()){
                Double curAvg = weekToAQI.get(weekYear);

                String[] vals = weekYear.split("-");
                Integer week = Integer.parseInt(vals[0]);
                Integer year = Integer.parseInt(vals[1]);
                String nextKey = (week+1) + "-" + year;
                if(!weekToAQI.containsKey(nextKey)){ //next week not in year
                    nextKey = "1-" + (year+1);
                }

                if(weekToAQI.containsKey(nextKey)){
                    Double nextAvg = weekToAQI.get(nextKey);
                    Double weekChange = curAvg - nextAvg;
                    changes.add(weekChange);
                }
            }

            Double maxDiff = changes.get(0);
            for(Double change : changes){
                if(Math.abs(change) > Math.abs(maxDiff)){
                    maxDiff = change;
                }
            }
            context.write(key, new DoubleWritable(maxDiff));
        }

    }
}