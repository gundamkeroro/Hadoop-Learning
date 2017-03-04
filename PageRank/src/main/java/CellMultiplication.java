import jdk.nashorn.internal.scripts.JO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.aggregate.DoubleValueSum;
import org.apache.hadoop.mapreduce.lib.aggregate.StringValueMax;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.ArrayListBackedIterator;
import org.apache.hadoop.mapreduce.lib.join.StreamBackedIterator;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by fengxinlin on 3/4/17.
 */
public class CellMultiplication {
    public static class TransitionDataMapper extends Mapper<Object, Text, Text, Text> {
    //The first mapper split data into from, tos K-V pairs
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String inputLine = value.toString().trim();
            String[] fromPageToPages = inputLine.split("\t");
            if (fromPageToPages.length == 1 || fromPageToPages[1].trim().equals("")) {
                return;
            }
            String from = fromPageToPages[0];
            String[] tos = fromPageToPages[1].split(",");
            for (String to : tos) {
                context.write(new Text(from), new Text(to + "=" + (double)1/tos.length));
            }
        }
    }

    public static class PageRankmapper extends Mapper<Object, Text, Text, Text> {
    //The second mapper split initial page rank
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pageRank = value.toString().trim().split("\t");
            context.write(new Text(pageRank[0]), new Text(pageRank[1]));
        }
    }

    public static class CellMultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> transitionMatrixCell = new ArrayList<String>();
            double pageRankCell = 0;
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    transitionMatrixCell.add(value.toString());
                } else {
                    pageRankCell = Double.parseDouble(value.toString());
                }
            }
            for (String cell : transitionMatrixCell) {
                String outputKey = cell.split("=")[0];
                double relation = Double.parseDouble(cell.split("0")[1]);
                String outputValue = String.valueOf(relation * pageRankCell);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJarByClass(CellMultiplication.class);

        ChainMapper.addMapper(job, TransitionDataMapper.class, Object.class,
                Text.class, Text.class, Text.class, config);
        ChainMapper.addMapper(job, PageRankmapper.class, Object.class,
                Text.class, Text.class, Text.class, config);

        job.setReducerClass(CellMultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PageRankmapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
