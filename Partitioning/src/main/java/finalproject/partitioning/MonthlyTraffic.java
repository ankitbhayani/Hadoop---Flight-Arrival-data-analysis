package finalproject.partitioning;


import java.io.IOException;
//import static java.lang.System.exit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author ankitbhayani
 */
public class MonthlyTraffic {

  public static class TMapper extends Mapper<Object, Text, Text, IntWritable>{
    //private final static IntWritable one = new IntWritable(1);
    private Text month= new Text();
    
    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
      
        String row[] = value.toString().split(","); 
        if (!value.toString().contains("UniqueCarrier")) {
        	if(!row[14].equalsIgnoreCase("NA")){
	            month.set(row[1]);
	            context.write(month, new IntWritable(Integer.parseInt(row[14])));
	            
	        }
        }
    }
  }

  public static class IReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,Context context) 
            throws IOException, InterruptedException {
            int sum=0;
            
            for (IntWritable val : values){
                sum +=val.get();
            }    
            result.set(sum);
            context.write(key, result);
    }
  }
  
  private static class LPartitioner extends Partitioner<Text, IntWritable>{

        @Override
        public int getPartition(Text key, IntWritable value, int numOfPartitions) {
            //int mon = 0;
            int mon = Integer.parseInt(key.toString());
            
            /*switch(key.toString()){
                case "Jan" :
                    mon=1;break;
                case "Feb" :
                    mon=2; break;
                case "Mar" :
                    mon=3; break;
                case "Apr" :
                    mon=4; break;    
                case "May" :
                    mon=5;break;
                case "Jun" :
                    mon=6; break;
                case "Jul" :
                    mon=7; break;
                case "Aug" :
                    mon=8; break;  
                case "Sep" :
                    mon=9; break;
                case "Oct" :
                    mon=10; break;
                case "Nov" :
                    mon=11; break;
                case "Dec" :
                    mon=12; break;       
                 
                default :
                    System.out.println("Erroneous Month");
                    exit(0);
                    
            }*/
            
            return (mon % numOfPartitions);
        }
  }
    
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Monthly Partitioning count");
            job.setJarByClass(MonthlyTraffic.class);
            
            job.setMapperClass(TMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            
            job.setPartitionerClass(LPartitioner.class);
            job.setNumReduceTasks(12);
            
            job.setCombinerClass(IReducer.class);
            job.setReducerClass(IReducer.class);    
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
