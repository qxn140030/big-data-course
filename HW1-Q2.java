import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
 
public class Q2 {
	
    public static class MutualFriendsMapper extends Mapper<Object, Text, Text, Text> {
        
    	private Text m_id = new Text();
        private Text m_friends = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	//use Context object get conf object
            Configuration conf = context. getConfiguration();
            String arg0 = conf.get("leftKey");   // e.g. arg0 = 0
            String arg1 = conf.get("rightKey");  // e.g. arg1 = 1
          
            // the key is null and the value is one line of our input file.
            // Split by \t to separate the user and its friends list.
            String line = value.toString();  // e.g. line = 0	1,2   
            String[] userAndFriends = line.split("\t");
            String user = userAndFriends[0];  // e.g. user = 0
            String id = arg0.compareTo(arg1) < 0 ? arg0 + "," + arg1 : arg1 + "," + arg0;  // e.g. id = (0,1) (0,2)
        	m_id.set(id);
            if(userAndFriends.length == 1 && (user.equals(arg0) || user.equals(arg1))) {
            	context.write(m_id, new Text());
            }
            if(userAndFriends.length == 2 && (user.equals(arg0) || user.equals(arg1))) {
	            String friends = userAndFriends[1];  // e.g. friends = {1,2}
	        	// For each friend in the list, output the (UserFriend, ListOfFriends) pair
	        	m_friends.set(friends);  // e.g. m_friends = (1,2)
	        	context.write(m_id, m_friends);  // e.g. key = (0, 1)  value = (1,2)                
            }	            
        }
    }
 
    public static class MutualFriendsReducer extends Reducer<Text, Text, Text, Text> {  
    	private Text r_result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Prepare a 2-String-Array to hold the values, i.e. the friends lists of our current friends pair.
            String[] preCombine = new String[2];
            int count = 0;
            for(Text value : values) {
            	preCombine[count++] = value.toString();
            }
 
            // Calculate the intersection of these lists and write result in the form (UserAUserB, MutualFriends).
            String r_str = intersection(preCombine[0], preCombine[1]);
            if(r_str != null) {
            	r_result.set(r_str);
            } else {
            	r_result.set(" ");
            }
            context.write(key, r_result);    
        }
        // Calculates intersection of two given Strings, i.e. friends lists
        private String intersection(String s1, String s2) {
            List<String> list1 = new ArrayList<>(Arrays.asList(s1.split(",")));  
            List<String> list2 = new ArrayList<>(Arrays.asList(s2.split(",")));
           
            list1.retainAll(list2);            
            String mutualFriends = StringUtils.join(",", list1);            
            return mutualFriends;
        }
    }
 
    public static void main(String args[]) throws Exception {
        // Standard Job setup procedure.
        Configuration conf = new Configuration();
        conf.set("leftKey", args[0]);
        conf.set("rightKey", args[1]);
        Job job = Job.getInstance(conf, "Q2");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(MutualFriendsMapper.class);
        job.setReducerClass(MutualFriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
