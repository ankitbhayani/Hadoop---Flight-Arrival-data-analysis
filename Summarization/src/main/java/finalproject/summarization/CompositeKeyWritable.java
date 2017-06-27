package finalproject.summarization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CompositeKeyWritable implements Writable{
	
    private int arrMinDelay;
    private int arrMaxDelay;
    private int depMinDelay;
    private int depMaxDelay;
	private double average;
	private long count;
	
	public CompositeKeyWritable(){}

    public CompositeKeyWritable(int arrMinDelay, int arrMaxDelay, int depMinDelay, 
    		int depMaxDelay, double average, int count) {
        this.arrMinDelay = arrMinDelay;
        this.arrMaxDelay = arrMaxDelay;
        this.depMinDelay = depMinDelay;
        this.depMaxDelay = depMaxDelay;
        this.average = average;
        this.count = count;
    }
	
	public int getArrMinDelay() {
		return arrMinDelay;
	}

	public void setArrMinDelay(int arrMinDelay) {
		this.arrMinDelay = arrMinDelay;
	}

	public int getArrMaxDelay() {
		return arrMaxDelay;
	}

	public void setArrMaxDelay(int arrMaxDelay) {
		this.arrMaxDelay = arrMaxDelay;
	}

	public int getDepMinDelay() {
		return depMinDelay;
	}

	public void setDepMinDelay(int depMinDelay) {
		this.depMinDelay = depMinDelay;
	}

	public int getDepMaxDelay() {
		return depMaxDelay;
	}

	public void setDepMaxDelay(int depMaxDelay) {
		this.depMaxDelay = depMaxDelay;
	}

	public double getAverage() {
		return average;
	}

	public void setAverage(double average) {
		this.average = average;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
	
	public void readFields(DataInput in) throws IOException {
		arrMinDelay = in.readInt();
		arrMaxDelay = in.readInt();
		depMinDelay = in.readInt();
		depMaxDelay = in.readInt();
		average = in.readDouble();
		count = in.readLong();
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(arrMinDelay);
		out.writeInt(arrMaxDelay);
		out.writeInt(depMinDelay);
		out.writeInt(depMaxDelay);
		out.writeDouble(average);
		out.writeLong(count);
	}

	@Override
	public String toString() {
		return (arrMinDelay +" " +arrMaxDelay +" "+ depMinDelay +" " + depMaxDelay +" " + average +" "+ count );
	}
	
	
}
