package Hw1_4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CKComparator extends WritableComparator {

    protected CKComparator() {
		super(UserAgeWritable.class, true);
    }
	
	public int compare(WritableComparable w1, WritableComparable w2) {
		UserAgeWritable ck1 = (UserAgeWritable)w1;
		UserAgeWritable ck2 = (UserAgeWritable)w2;
		Integer userid1 = new Integer(ck1.getUserId());
		Integer userid2 = new Integer(ck2.getUserId());;
		Float avgAge1 = ck1.getAvgAge();
		Float avgAge2 = ck2.getAvgAge();
		int rst = -1 * avgAge1.compareTo(avgAge2);
		if (rst == 0) {
			return userid1.compareTo(userid2);
		}
		else {
			return rst;
		}
	}
}
