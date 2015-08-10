package kmeansclustering;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PointWritable implements Writable
{
	private double _x;
	private double _y;

	public PointWritable()
	{
		this(0, 0);
	}

	public PointWritable(double x, double y)
	{
		_x = x;
		_y = y;
	}

	public void setX(double x)
	{
		_x = x;
	}

	public double getX()
	{
		return _x;
	}

	public void setY(double y)
	{
		_y = y;
	}

	public double getY()
	{
		return _y;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException
	{
		dataOutput.writeDouble(_x);
		dataOutput.writeDouble(_y);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException
	{
		_x = dataInput.readDouble();
		_y = dataInput.readDouble();
	}

	@Override
	public int hashCode()
	{
		return (int)Double.doubleToLongBits(_x) ^ (int)Double.doubleToLongBits(_y);
	}

	public PointWritable clonePoint()
	{
		return new PointWritable(_x, _y);
	}

	@Override
	public String toString()
	{
		return Double.toString(_x) + " " + Double.toString(_y);
	}

	public void parse(String line)
	{
		int index = line.indexOf(' ');
		String xString = line.substring(0, index);
		String yString = line.substring(index).trim();

		double x = Double.parseDouble(xString);
		double y = Double.parseDouble(yString);

		this.setX(x);
		this.setY(y);
	}

	public static PointWritable parsePoint(String line)
	{
		PointWritable point = new PointWritable();
		point.parse(line);
		return point;
	}

	public double distance2To(PointWritable center)
	{
		double xx = center.getX() - this.getX();
		double yy = center.getY() - this.getY();
		return xx * xx + yy * yy;
	}
}
