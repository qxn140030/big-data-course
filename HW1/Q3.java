import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Use in-memory join. Given any two Users as input, output the list of the names and the zipcode of their mutual friends. 
 */
 
public class Q3 {
	
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
  	private Text m_id = new Text();
  	private Text m_friends = new Text();
  	HashMap<String,String> map;
		
	  protected void setup(Context context) throws IOException, InterruptedException {			
	    super.setup(context);
	    //read data to memory on the mapper.
	    map = new HashMap<String,String>();
	    Configuration conf = context.getConfiguration();
	    String userdataPath = conf.get("userdata");
	    Path path = new Path("hdfs://cshadoop1" + userdataPath);       //Location of file in HDFS
						
	    FileSystem fs = FileSystem.get(conf);
	    FileStatus[] fsta = fs.listStatus(path);
	    for (FileStatus status : fsta) {
        	Path pt = status.getPath();
		        
	        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
	        String line;
	        line = br.readLine();
	        while (line != null) {
        	  String[] arr = line.split(",");
        	  if (arr.length == 10) {
        		  map.put(arr[0].trim(), line); //user
        	  }
            line = br.readLine();
	        }		       
	    }
	}
		
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
  	  //use Context object get conf object
      Configuration conf = context. getConfiguration();
      String arg0 = conf.get("leftKey");
      String arg1 = conf.get("rightKey");
	    // In our case, the key is null and the value is one line of our input file.
	    // Split by \t to separate the user and its friends list.
  	  String line = value.toString();                     // e.g. line = 0	1,2   
      String[] userAndFriends = line.split("\t");
      String user = userAndFriends[0];                    // e.g. user = 0
      String id = arg0.compareTo(arg1) < 0 ? arg0 + "," + arg1 : arg1 + "," + arg0;       // e.g. id = (0,1) (0,2)
      m_id.set(id);
      if (userAndFriends.length == 1 && (user.equals(arg0) || user.equals(arg1))) {
      	context.write(m_id, new Text());
      }
      if (userAndFriends.length == 2 && (user.equals(arg0) || user.equals(arg1))) {
      	  // For each friend in the list, output the (UserFriend, ListOfFriends) pair
          String[] friends = userAndFriends[1].split(",");
		      List<String> friendInfoList = new ArrayList<>();
		      for (String mututalFriend : friends) {
      			String[] friendAttributes = map.get(mututalFriend).split(",");
      			String friendInfo = friendAttributes[1] + ":" + friendAttributes[6]; 
      			friendInfoList.add(friendInfo);
      		}
  		    m_friends.set(StringUtils.join(friendInfoList, ","));
  		    context.write(m_id, m_friends);
      }
	  }
  } 

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    	private Text r_result = new Text();
    	// Calculates intersection of two given Strings, i.e. friends lists
    	private String intersection(String s1, String s2) {
    	    List<String> list1 = new ArrayList<>(Arrays.asList(s1.split(",")));
    	    List<String> list2 = new ArrayList<>(Arrays.asList(s2.split(",")));
    	    list1.retainAll(list2);
    	    return list1.toString();
	    }

	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	    // Prepare a 2-String-Array to hold the values, i.e. the friends lists of our current friends pair.
    	    String[] preCombine = new String[2];
     	    int count = 0;
    	    for (Text value : values) {
		          preCombine[count++] = value.toString();
	        }
    	    // Calculate the intersection of these lists and write result in the form (UserAUserB, MutualFriends).
    	    r_result.set(intersection(preCombine[0], preCombine[1]));
    	    context.write(key, r_result);
	    }
  }

  public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	conf.set("leftKey", args[0]);
    	conf.set("rightKey", args[1]);        
    	conf.set("userdata", args[2]);
    	Job job = Job.getInstance(conf, "Q3");
    	
    	job.setJarByClass(FriendInfoFinder.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    
    	job.setMapperClass(Map.class);
    	job.setReducerClass(Reduce.class);
    
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    
    	FileInputFormat.addInputPath(job, new Path(args[3]));
    	FileOutputFormat.setOutputPath(job, new Path(args[4]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
