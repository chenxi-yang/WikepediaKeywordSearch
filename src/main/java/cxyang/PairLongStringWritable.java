package cxyang;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class PairLongStringWritable implements Writable{

    public long x;
    public String y;

    public PairLongStringWritable() { super();}

    public PairLongStringWritable(long _x, String _y) {
        x = _x;
        y = _y;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(x);
        out.writeUTF(y);
    }

    public void readFields(DataInput in) throws IOException {
        x = in.readLong();
        y = in.readUTF();
    }

    @Override
    public String toString() {
        return String.valueOf(x) + "," + y;
    }

}