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
 * QUESTÃO 2 – Número de transações por ano.
 *
 *   Map:    (ano, 1)          chave IntWritable -> o Hadoop ordena os anos numericamente
 *   Reduce: (ano, soma)
 */
public class Q2TransacoesPorAno {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        System.exit(executar(new Configuration()) ? 0 : 1);
    }

    public static boolean executar(Configuration c) throws Exception {
        Path input = Caminhos.ENTRADA;
        Path output = Caminhos.saida("q2");
        Caminhos.limparSaida(c, output);

        Job j = Job.getInstance(c, "q2-transacoes-por-ano");

        j.setJarByClass(Q2TransacoesPorAno.class);
        j.setMapperClass(MapQ2.class);
        j.setCombinerClass(ReduceQ2.class);
        j.setReducerClass(ReduceQ2.class);

        j.setMapOutputKeyClass(IntWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        return j.waitForCompletion(true);
    }

    public static class MapQ2 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private final IntWritable um = new IntWritable(1);

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            Transacao t = Transacao.ler(value, con);
            if (t == null) return;

            con.write(new IntWritable(t.getAno()), um);
        }
    }

    public static class ReduceQ2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable v : values) {
                soma += v.get();
            }
            con.write(key, new IntWritable(soma));
        }
    }
}
