package session6.assignment2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IndOlympicsMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\t");

		if (lineArray[2].equals("India")) {

			IntWritable year = new IntWritable(Integer.parseInt(lineArray[3]));
			IntWritable medals = new IntWritable(Integer.parseInt(lineArray[9]));

			context.write(year, medals);

		}

	}

}
