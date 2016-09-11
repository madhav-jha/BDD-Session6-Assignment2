package session6.assignment2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class IndOlympicsReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int totalMedals = 0;
		for (IntWritable value : values) {
			totalMedals = totalMedals + value.get();
		}

		context.write(key, new IntWritable(totalMedals));
	}

}
