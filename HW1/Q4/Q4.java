package Hw1_4;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/*
 * Using reduce-side join and job chaining:
 * Step 1: Calculate the average age of the direct friends of each user. 
 * Step 2: Sort the users by the calculated average age from step 1 in descending order. 
 * Step 3. Output the top 20 users from step 2 with their address and the calculated average age.
 */
public class Q4 {
	  private static int counter = 0;
    public static class Map1 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private IntWritable m_id = new IntWritable();
        private IntWritable m_Age = new IntWritable();
        HashMap<Integer, Integer> ageMap;
		
    		protected void setup(Context context) throws IOException, InterruptedException {			
      			super.setup(context);
      			// read userdata to memory on the mapper.
      			ageMap = new HashMap<Integer, Integer>();
      			Configuration conf = context.getConfiguration();
      			String userdataPath = conf.get("userdata");
      			
      			Path p = new Path("hdfs://cshadoop1" + userdataPath);      // Location of file in HDFS
      			
      			FileSystem fs = FileSystem.get(conf);
      			FileStatus[] fss = fs.listStatus(p);
		        for (FileStatus status : fss) {
    		        Path pth = status.getPath();
    		        
    		        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pth)));
    		        String line;
    		        line = br.readLine();
    		        while (line != null){
      		        	String[] a = line.split(",");
      		        	if (a.length == 10) {
      		        		  ageMap.put(Integer.parseInt(a[0].trim()), getAge(a[9]));  // user
      		        	}
      		          line = br.readLine();
    		        }
		        }
		    }
        // HashMap<K, V> ageMap K- UID V - GETAGE(birthday)
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] userAndFriends = line.split("\t");
            String user = userAndFriends[0];
            m_id.set(Integer.parseInt(user));
            
            if (userAndFriends.length == 2) {               
                // uid, [fuid, fuid, ....]
                // emit(uid, ageMap.get(fuid))
                String[] friends = userAndFriends[1].split(",");
                for (String fuid : friends) {
                  	m_Age.set(ageMap.get(Integer.parseInt(fuid)));
                  	context.write(m_id, m_Age);
                }                
            }
        }
		    private int getAge(String birthday) {			
      			SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
      			Calendar cal = Calendar.getInstance();
      			if (cal.before(birthday)) {
      				  throw new IllegalArgumentException("not a valid birthday");
      		  }
      			int yearNow = cal.get(Calendar.YEAR);
  	        int monthNow = cal.get(Calendar.MONTH); //
  	        int dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);
  	        try {
      			    cal.setTime(dateFormat.parse(birthday));
      			} catch (ParseException e) {
        				// TODO Auto-generated catch block
        				e.printStackTrace();
      			}	       
  	        int yearBirth = cal.get(Calendar.YEAR);
  	        int monthBirth = cal.get(Calendar.MONTH);
  	        int dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);
  
  	        int age = yearNow - yearBirth;	        
  	        if (monthNow <= monthBirth) {
	            if (monthNow == monthBirth) {	                
	               if (dayOfMonthNow < dayOfMonthBirth) {
	                  age--;
	               } 
	            } else {
	                //monthNow>monthBirth
	                age--;
	            }
	          } 
	          return age;
		   }	
    } 
    // reducer1  calculate average   -> <uid, avgAgeOfFriends>
    // -> output1
    //      |
    //  Mapper2

    // calculate average age of the friend list
    public static class Reduce1 extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable> {
      	private IntWritable user_id = new IntWritable();
      	private FloatWritable r_Age = new FloatWritable();
	      //	private TreeMap<Float, String> ageTreeMap = new TreeMap<>();
		
		    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      			float sum = 0;
      			int count = 0;
		        //	HashMap<String, String> myMap = new HashMap<String, String>();
    		    for (IntWritable value : values) {
    		    	sum += value.get();
    	    		count++;	    				    		    	
    		    }
    		    user_id.set(key.get());
    		    r_Age.set(sum / count);
    		    context.write(user_id, r_Age);		    
		    }			    
    }
  	// emit key = (user_id + user_age), value = NullWritable
  	public static class Map2 extends Mapper<LongWritable, Text, UserAgeWritable, NullWritable> {
  		private UserAgeWritable m_user = new UserAgeWritable();
		
  		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
  			String line = value.toString();
  			String[] a = line.split("\t");
  			if (a.length == 2) {
  				m_user.setUserId(Integer.parseInt(a[0]));	
  				m_user.setAvgAge(Float.parseFloat(a[1]));	
  				context.write(m_user, NullWritable.get());
			  }
		  }
	  }
	
	public static class Reduce2 extends Reducer<UserAgeWritable, NullWritable, IntWritable, FloatWritable> {
		private IntWritable r_id = new IntWritable();
		private FloatWritable r_avgAge = new FloatWritable();
		public void reduce(UserAgeWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			r_id.set(key.getUserId());
			r_avgAge.set(key.getAvgAge());
			context.write(r_id, r_avgAge);
		}
	}
	public static class Map3_ad extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable m_userid = new IntWritable();
		private Text m_address = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] a = line.split(",");
			if (a.length == 10) {
				String address = "add," + a[3] + "," + a[4] + "," + a[5];
				m_userid.set(Integer.parseInt(a[0].trim()));
				m_address.set(address);
				context.write(m_userid, m_address);
			}
		}
	}
	public static class Map3_ag extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable m_user = new IntWritable();
		private Text m_age = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if(counter++ < 20) {
				String line = value.toString();
				String[] a = line.split("\t");
				if (a.length == 2){					
					m_user.set(Integer.parseInt(a[0]));
					m_age.set("age," + a[1]);
					context.write(m_user, m_age);
				}				
			}
		}
	}
	// reduce-side join
  public static class Reduce3 extends Reducer<IntWritable, Text, IntWritable, Text> {
		private IntWritable user_id = new IntWritable();
		private Text r_rst = new Text();
		private PriorityQueue<UserAgeWritable> queue = new PriorityQueue<>();
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] allInfo = new String[2];
			for (Text value : values) {
				String val = value.toString();
				String tag = val.substring(0, 3);
				String afterTag = val.substring(4);
				switch(tag) {
				case "add" :
					allInfo[0] = afterTag;
					break;
				case "age":
					allInfo[1] = afterTag;
					break;
				default : 
					break;
				}
			}
			if (allInfo[1] != null) {					
				queue.offer(new UserAgeWritable(key.get(), Float.parseFloat(allInfo[1]), allInfo[0]));				
			}
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while (!queue.isEmpty()) {
				UserAgeWritable result = queue.poll();
				user_id.set(result.getUserId());
				r_rst.set(result.getAddress() + "," + result.getAvgAge());
				context.write(user_id, r_rst);
			}
		}		    
  }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
               
		conf.set("userdata", args[0]);
		Job job1 = Job.getInstance(conf, "job1");
		
		job1.setJarByClass(Q4.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(FloatWritable.class);

		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));

		job1.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "job2");		
		job2.setJarByClass(Q4.class);
		job2.setMapOutputKeyClass(UserAgeWritable.class);
		job2.setMapOutputValueClass(NullWritable.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(FloatWritable.class);
		
		job2.setPartitionerClass(KeyPartitioner.class);
		job2.setGroupingComparatorClass(KeyGroupingComparator.class);
    job2.setSortComparatorClass(CKComparator.class);
    
    job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));

		job2.waitForCompletion(true);
		
		Job job3 = Job.getInstance(conf, "job3");		
		job3.setJarByClass(Q4.class);
		job3.setMapOutputKeyClass(IntWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job3, new Path(args[0]), TextInputFormat.class, Map3_ad.class);
		MultipleInputs.addInputPath(job3, new Path(args[3]), TextInputFormat.class, Map3_ag.class);
		
		job3.setReducerClass(Reduce3.class);

		job3.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job3, new Path(args[4]));

		job3.waitForCompletion(true);
	}
}
