package pjbl01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * QUESTÃO 4 – Número de transações por tipo de fluxo (Export, Import, Re-Export, Re-Import).
 *
 *   Map:    (fluxo, 1)
 *   Reduce: (fluxo, soma)
 */
public class Q4TransacoesPorFluxo {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        System.exit(executar(new Configuration()) ? 0 : 1);
    }

    public static boolean executar(Configuration c) throws Exception {
        Path input = Caminhos.ENTRADA;
        Path output = Caminhos.saida("q4");
        Caminhos.limparSaida(c, output);

        Job j = Job.getInstance(c, "q4-transacoes-por-fluxo");

        j.setJarByClass(Q4TransacoesPorFluxo.class);
        j.setMapperClass(MapQ4.class);
        j.setCombinerClass(ReduceQ4.class);
        j.setReducerClass(ReduceQ4.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        return j.waitForCompletion(true);
    }

    public static class MapQ4 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable um = new IntWritable(1);

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            Transacao t = Transacao.ler(value, con);
            if (t == null) return;

            con.write(new Text(t.getFluxo()), um);
        }
    }

    public static class ReduceQ4 extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable v : values) {
                soma += v.get();
            }
            con.write(key, new IntWritable(soma));
        }
    }
}
