package finalproject.binning;

/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
*
* @author ankitbhayani
*/
public class FlightCancellationReasonBinning {

public static class TMapper extends Mapper<Object, Text, Text, NullWritable>{

private MultipleOutputs<Text, NullWritable> mos=null;  
//private final static IntWritable one = new IntWritable(1);
//private Text hour= new Text();

@Override
protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs(context);
 }

 @Override
 public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {
  
    String row[] = value.toString().split(","); 
    if (!value.toString().contains("UniqueCarrier")) {
        if(!row[21].equalsIgnoreCase("0")){
	        String cancellationCode = row[22];
	        
	        if(cancellationCode.equalsIgnoreCase("A"))
	            mos.write("bins", value, NullWritable.get(),"Carrier-cancellation");
	        if(cancellationCode.equalsIgnoreCase("B"))
	            mos.write("bins", value, NullWritable.get(),"Weather-cancellation");
	        if(cancellationCode.equalsIgnoreCase("C"))
	            mos.write("bins", value, NullWritable.get(),"NAS-cancellation");
	        if(cancellationCode.equalsIgnoreCase("D"))
	            mos.write("bins", value, NullWritable.get(),"Security-cancellation");
    	}
    }
}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

}





public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cancellation Binning ");
        job.setJarByClass(FlightCancellationReasonBinning.class);
        
        job.setMapperClass(TMapper.class);
        
        
        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);
        job.setNumReduceTasks(0);
         
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
