/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalproject.top10;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author ankitbhayani
 */
public class Top10BusiestAirport {

    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable>{

        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
        	if (!value.toString().contains("UniqueCarrier")) {
	            
        		String row[] = value.toString().split(",");
        		if(!row[14].equalsIgnoreCase("NA")){
	        		
		            String destAirport = row[17];
		            
		            //String arrDelay = row[14].trim();
		            
		            try {
		                IntWritable busyRating = new IntWritable(Integer.parseInt(row[14]));
		                context.write(new Text(destAirport), busyRating);
		
		            } catch (Exception e) {
		                e.printStackTrace();
		            }
        		}     
            
         }
        	
      }   
        
    }
    
    
    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
            //int count=0;
            int sum=0; 
            //double avg=0;
            
            for(IntWritable val : values){
                sum += val.get();
                //++count;
            }
           
            //avg=sum/count;
            result.set(sum);
            context.write(key, result);
        }
        
        
        
    }
    
    
     public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text>{

        //private final static IntWritable sales = new IntWritable();
        //private Text employee = new Text();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
            
            String row[] = value.toString().split("\\t");
            //IntWritable movieId = new IntWritable(Integer.parseInt(row[0]));
            //Text airport = new Text(row[0]);
            
            String rating = row[1].trim();
            
            try {
                IntWritable ratingg = new IntWritable(Integer.parseInt(rating));
                context.write(ratingg, new Text(row[0]));

            } catch (Exception e) {
                e.printStackTrace();
            }
            
        }
   
        
    }
     
    public static class Reduce2 extends Reducer<IntWritable, Text, Text, IntWritable>{

        private static int count= 25; 
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
            for(Text val : values){
                if(count>0){
                    context.write(val, key);
                    --count;
                }    
                else
                   break; 
            }
            
        }

            
        
    }
    
    
    public static class BusyRatingMapper extends Mapper<Object, Text, Text, Text>{
    	 
        private Text outkey= new Text();
        private Text outValue= new Text();
        
        @Override
         public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
          
            String airportList[] = value.toString().split("\t");
            String airport = airportList[0].replace("\"", "");
            
            if(airport!=null){
                outkey.set(airport);
                outValue.set("A"+ value.toString().replace("\"", ""));
                context.write(outkey, outValue);
            }
        }
      }

     public static class AiportMapper extends Mapper<Object, Text, Text, Text>{
     
        private Text outkey= new Text();
        private Text outValue= new Text();
        
        @Override
         public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
          
            String aiportList[] = value.toString().split(",");
            
            if (!value.toString().replace("\"", "").contains("iata")) {
	            String iata = aiportList[0].replace("\"", "");
	            //System.out.println("Airport nm ->" + iata);
	            if(iata!=null){
	                outkey.set(iata);
	                outValue.set("B"+ value.toString().replace("\"", "").replace(",", " "));
	                context.write(outkey, outValue);
	            }
            }
        }
      }

     
      public static class JoinReducer extends Reducer<Text,Text,Text,Text> {
        
            public static final Text EMPTY_TEXT = new Text("");
            private Text tmp = new Text();
            private ArrayList<Text> listA = new ArrayList<Text>();
            private ArrayList<Text> listB= new ArrayList<Text>();
            private String joinType = null;

            @Override
            protected void setup(Context context) throws IOException, InterruptedException {
                joinType= context.getConfiguration().get("join.type");
            }

       
            @Override
            public void reduce(Text key, Iterable<Text> values,Context context) 
                    throws IOException, InterruptedException {
                    
                  listA.clear();
                  listB.clear();
                  
                  for(Text val: values){
                      tmp = val;
                      
                      if(tmp.charAt(0) == 'A'){
                          listA.add(new Text(tmp.toString().substring(1)));
                      }   
                      else if(tmp.charAt(0) == 'B'){
                          listB.add(new Text(tmp.toString().substring(1)));
                      }    
                  }
                  
                  executeJoin(context);
                  
            }
        
           
            private void executeJoin(Context context) 
                throws IOException, InterruptedException{
                if(joinType.equalsIgnoreCase("inner")){
                    if(!listA.isEmpty() && !listB.isEmpty()){
                        for(Text A: listA){
                            for(Text B: listB){
                                //System.out.println("ListAB contains : "+ A + " " + B);
                                context.write(A, B);
                            }
                        }
                    }
                }
            }

        
      }
     
    
    
     /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
       
            Configuration conf1 = new Configuration();
            Job job1 = Job.getInstance(conf1, "Chaining");
            job1.setJarByClass(Top10BusiestAirport.class);
           
            job1.setMapperClass(Map1.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(IntWritable.class);
            
            
            job1.setReducerClass(Reduce1.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);
            job1.setCombinerClass(Reduce1.class);
            
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));
            
            boolean firstComplete = job1.waitForCompletion(true);
            boolean secondComplete=false;
            
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Chaining");
            
            
            
             if(firstComplete) {
                 
                job2.setJarByClass(Top10BusiestAirport.class); 
                job2.setMapperClass(Map2.class);
                job2.setMapOutputKeyClass(IntWritable.class);
                job2.setMapOutputValueClass(Text.class);


                job2.setReducerClass(Reduce2.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(IntWritable.class);
                
                job2.setSortComparatorClass(SortKeyComparator.class);
		
                job2.setNumReduceTasks(1);
               
                FileInputFormat.addInputPath(job2, new Path(args[1]));
                FileOutputFormat.setOutputPath(job2, new Path(args[2]));
                
                secondComplete=job2.waitForCompletion(true);
                
             }
             
             Configuration conf3 = new Configuration();
             Job job3 = Job.getInstance(conf3, "Join");
             
             
             if(secondComplete){
            	 
            	 job3.setJarByClass(Top10BusiestAirport.class); 
                 job3.setMapperClass(BusyRatingMapper.class);
                 job3.setMapperClass(AiportMapper.class);
                 //job3.setMapOutputKeyClass(Text.class);
                 //job3.setMapOutputValueClass(Text.class);
                 job3.setReducerClass(JoinReducer.class);

                 MultipleInputs.addInputPath(job3, new Path(args[2]), TextInputFormat.class, BusyRatingMapper.class);
                 MultipleInputs.addInputPath(job3, new Path(args[3]), TextInputFormat.class, AiportMapper.class);
                 job3.getConfiguration().set("join.type", "inner");
                 
                 
                 job3.setOutputKeyClass(Text.class);
                 job3.setOutputValueClass(Text.class);
                 
                         
                 FileInputFormat.addInputPath(job3, new Path(args[2]));
                 FileOutputFormat.setOutputPath(job3, new Path(args[4]));
            	 
            	 System.exit(job3.waitForCompletion(true)? 0 :1);
             }
             
             
             
    }
    
    
}
