package Hw1_4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserAgeWritable implements WritableComparable<UserAgeWritable> {
	int userId;
	float avgAge;
	String address;
	
	public UserAgeWritable(int userId, float avgAge, String address) {
		super();
		this.userId = userId;
		this.avgAge = avgAge;
		this.address = address;
	}

	public UserAgeWritable() {
		super();
	}

	public int getUserId() {
		return userId;
	}

	public float getAvgAge() {
		return avgAge;
	}
	
	public String getAddress() {
		return address;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public void setAvgAge(float avgAge) {
		this.avgAge = avgAge;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public void readFields(DataInput in) throws IOException {
		userId = Integer.parseInt(in.readUTF());
		avgAge = Float.parseFloat(in.readUTF());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(Integer.toString(userId));
		out.writeUTF(Double.toString(avgAge));
	}

	@Override
	public int compareTo(UserAgeWritable o) {
		int result = new Float(o.avgAge).compareTo(new Float(this.avgAge));
		if(result == 0) {
			return new Integer(this.userId).compareTo(new Integer(o.userId));
		}
		else {
			return result;
		}
	}
}
