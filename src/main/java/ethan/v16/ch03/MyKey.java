package ethan.v16.ch03;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @since 2014-9-22
 * @author ethan
 */
//可序列化 可比较
public class MyKey implements WritableComparable<MyKey>  {

	public final Text first;
	public final FloatWritable second;
	
	public MyKey(){
		first = new Text();
		second = new FloatWritable();
	}
	public MyKey(Text first,FloatWritable second){
		this.first = first;
		this.second = second;
	}

	public void write(DataOutput out) throws IOException {
		second.write(out);
		first.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		second.readFields(in);
		first.readFields(in);
	}
	public int compareTo(MyKey o) {
		 //first相等情况下，再比较second,first升序，second降序 
//		int fcp = first.compareTo(o.first);
//		if(fcp!=0){
//			return fcp;
//		}
		return -second.compareTo(o.second);
	}
	
	@Override
	public String toString() {
		
		return first +"|" + second;
	}

}
