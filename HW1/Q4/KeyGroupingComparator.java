package Hw1_4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyGroupingComparator extends WritableComparator {

	protected KeyGroupingComparator() {
		super(UserAgeWritable.class, true);
	}
	
	public int compare(WritableComparable w1, WritableComparable w2) {
		UserAgeWritable kg1 = (UserAgeWritable)w1;
		UserAgeWritable kg2 = (UserAgeWritable)w2;
		Integer userid1 = new Integer(kg1.getUserId());
		Integer userid2 = new Integer(kg2.getUserId());
		return userid1.compareTo(userid2);
	}
}
