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




public class Question4 {
	public static class MapperQ4 extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            String desiredYear = "2020";

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
            String year = sdf.format(Long.parseLong(line[2]));

            if(year.equals(desiredYear)){
                context.write(new Text(line[0]), new IntWritable(Integer.parseInt(line[1])));
            }
            
                
        }
	
    }
    public static class NodeComparator implements Comparator<Map.Entry<Text, Double>>{

        @Override
        public int compare(Map.Entry<Text, Double> node1, Map.Entry<Text, Double> node2){
    
                int compareValue = node1.getValue().compareTo(node2.getValue());
                if(compareValue > 0){ // was == 1
                    return 1;
                }
                else if(compareValue == 0){  
                    int compareKey =  node1.getKey().toString().compareTo(node2.getKey().toString()); 
                    if(compareKey > 0){ //was == 1
                        return -1;
                    }
                    else if(compareKey < 0){ // was == -1
                        return 1;
                    }
                    else{
                        return 0;
                    }
                }
                else if(compareValue < 0){ // was == -1
                    return -1;
                }
                else{
                    return 0; 
                }
        }
    
    }


    public static class ReducerQ4 extends Reducer<Text, IntWritable, Text, DoubleWritable>{
        private List<Map.Entry<Text, Double>> highestAverage = new ArrayList<Map.Entry<Text, Double>>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int length = 0;
            for(IntWritable x : values){
                sum += x.get();
                length++;
            }
            double average = (double)sum/length;

            highestAverage.add(new AbstractMap.SimpleEntry<Text, Double>(new Text(key), average));
            Collections.sort(highestAverage,Collections.reverseOrder(new  NodeComparator()));

            if(highestAverage.size() > 10){
                highestAverage.remove(highestAverage.size() - 1);
            }
        }
    
       @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<Text, Double> entry : highestAverage){
                Text county = new Text(entry.getKey());
                DoubleWritable average = new DoubleWritable(entry.getValue());
                context.write(county, average);
            }

        }

    }

    

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); 
		Job job = Job.getInstance(conf, "Question4"); 
		job.setJarByClass(Question4.class); 
		job.setMapperClass(Question4.MapperQ4.class); 
		job.setReducerClass(Question4.ReducerQ4.class); 
		job.setNumReduceTasks(1); 
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);  
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}