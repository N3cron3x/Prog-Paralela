package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount extends Configured implements Tool {

    // ── MAPPER ──────────────────────────────────────────────────────────────
    public static class WordMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            // Strip non-alphabetic characters, keep spaces
            line = line.replaceAll("[^a-záéíóúüñ\\s]", "");

            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().trim();
                if (!token.isEmpty()) {
                    word.set(token);
                    context.write(word, ONE);
                }
            }
        }
    }

    // ── COMBINER ────────────────────────────────────────────────────────────
    // IntSumReducer acts as Combiner: pre-agrega localmente antes de shuffle.
    // Reduce tráfico de red significativamente en datasets grandes.

    // ── REDUCER ─────────────────────────────────────────────────────────────
    public static class WordReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable total = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            total.set(sum);
            context.write(key, total);
        }
    }

    // ── DRIVER ──────────────────────────────────────────────────────────────
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: WordCount <input_hdfs_path> <output_hdfs_path> [num_reducers]");
            return 1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "WordCount");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(IntSumReducer.class);   // Combiner = mismo que Reducer
        job.setReducerClass(WordReducer.class);

        // Número de reducers configurable (default: 1)
        int numReducers = args.length >= 3 ? Integer.parseInt(args[2]) : 1;
        job.setNumReduceTasks(numReducers);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path inputPath  = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // Elimina output previo para evitar conflicto
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("[WordCount] Output anterior eliminado: " + outputPath);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long elapsed = System.currentTimeMillis() - startTime;

        System.out.printf("[WordCount] Reducers=%d | Tiempo=%dms | Estado=%s%n",
                numReducers, elapsed, success ? "EXITOSO" : "FALLIDO");

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(exitCode);
    }
}
