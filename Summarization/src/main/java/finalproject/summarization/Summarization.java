package finalproject.summarization;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Summarization {
	
	public static class Summarization_Mapper extends Mapper<Object, Text, Text, CompositeKeyWritable>{

		private Text UniqueCarrier = new Text();
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, CompositeKeyWritable>.Context context)
				throws IOException, InterruptedException {
			
			if (!value.toString().contains("UniqueCarrier")) {
                String[] input = value.toString().split(",");
                
                 
                if(!input[14].equalsIgnoreCase("NA") && !input[15].equalsIgnoreCase("NA")){
                	
                	double carrierDelay = 0;
                	if(input[15].equalsIgnoreCase("NA")) carrierDelay=0.0;
                	else carrierDelay=Double.parseDouble(input[15]);
                		
                	CompositeKeyWritable outTuple= new CompositeKeyWritable(Integer.parseInt(input[14]),Integer.parseInt(input[14]),
                    		Integer.parseInt(input[15]),Integer.parseInt(input[15]),carrierDelay,1);
                    	UniqueCarrier.set(input[8]);
                    	context.write(UniqueCarrier, outTuple);
                }
            }
		}
		
	}
	
	
	public static class Summarization_Reducer extends Reducer<Text, CompositeKeyWritable, Text, CompositeKeyWritable> {

		@Override
		protected void reduce(Text key, Iterable<CompositeKeyWritable> values,Reducer<Text, CompositeKeyWritable, Text, CompositeKeyWritable>.Context context)
						throws IOException, InterruptedException {
			    int minArrDel = Integer.MAX_VALUE;
		        int maxArrDel = Integer.MIN_VALUE;
			    int minDepDel = Integer.MAX_VALUE;
		        int maxDepDel = Integer.MIN_VALUE;
		        int count = 0;
				double sum = 0;
		   

		        for (CompositeKeyWritable val : values) {
		            
		        	if (val.getArrMinDelay() < minArrDel)
		            	minArrDel = val.getArrMinDelay();
		                
		            if (val.getArrMaxDelay() > maxArrDel) 
		            	maxArrDel = val.getArrMaxDelay();
		            
		            if (val.getDepMinDelay() < minDepDel)
		        		minDepDel = val.getDepMinDelay();
		                
		            if (val.getDepMaxDelay() > maxDepDel) 
		            	maxDepDel = val.getDepMaxDelay();
		            
		            sum += val.getAverage()*val.getCount();
					count +=val.getCount();

		        }

		        context.write(key,new CompositeKeyWritable(minArrDel, maxArrDel, minDepDel, maxDepDel,(sum/count),count));
		}
		
	}
	
	
    public static void main( String[] args ){
        System.out.println( "Hello World!" );
        
        Configuration conf = new Configuration();
	    
		try {
			Job job = Job.getInstance(conf, "stock price high");
			job.setJarByClass(Summarization.class);
			job.setMapperClass(Summarization_Mapper.class);
			job.setCombinerClass(Summarization_Reducer.class);
			job.setReducerClass(Summarization_Reducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(CompositeKeyWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
        
        
    }
}
