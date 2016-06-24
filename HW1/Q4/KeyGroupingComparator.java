package Hw1_4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyGroupingComparator extends WritableComparator {

	protected KeyGroupingComparator() {
		super(UserAgeWritable.class, true);
	}
	
	public int compare(WritableComparable e1, WritableComparable e2) {
		UserAgeWritable kg1 = (UserAgeWritable)e1;
		UserAgeWritable kg2 = (UserAgeWritable)e2;
		Integer userid1 = new Integer(kg1.getUserId());
		Integer userid2 = new Integer(kg2.getUserId());
		return userid1.compareTo(userid2);
	}
}
