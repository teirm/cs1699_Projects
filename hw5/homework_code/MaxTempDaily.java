// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTempDaily {

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: MaxTemperature <input path> <intermediate path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(MaxTempDaily.class);
    job.setJobName("Max Daily temperature");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureDailyMapper.class);
    job.setReducerClass(MaxTemperatureDailyReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    if (job.waitForCompletion(true)) {
    
            Job job2 = new Job();
            job2.setJarByClass(MaxTempDaily.class);
            job2.setJobName("Mean temperature");

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            
            job2.setMapperClass(MeanTemperatureMapper.class);
            job2.setReducerClass(MeanTemperatureReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            
            System.exit(job2.waitForCompletion(true) ? 0 : 1); 
            
    }
  }
}
// ^^ MaxTemperature
