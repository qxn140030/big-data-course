package Hw1_4;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<UserAgeWritable, NullWritable> {
	
	public int getPartition(UserAgeWritable key, NullWritable val, int numPartitions) {
		int hc = new Integer(key.getUserId()).hashCode();
		int partition = hc % numPartitions;
		return partition;
	}
}
