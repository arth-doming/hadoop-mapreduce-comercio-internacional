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
 * QUESTÃO 1 – Número de transações envolvendo o Brasil.
 *
 * Ideia (igual ao WordCount):
 *   Map:    para cada linha do Brasil emite ("Brazil", 1)
 *   Reduce: soma todos os 1  ->  ("Brazil", total)
 */
public class Q1TransacoesBrasil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        System.exit(executar(new Configuration()) ? 0 : 1);
    }

    public static boolean executar(Configuration c) throws Exception {
        Path input = Caminhos.ENTRADA;
        Path output = Caminhos.saida("q1");
        Caminhos.limparSaida(c, output);

        Job j = Job.getInstance(c, "q1-transacoes-brasil");

        // 1. registro das classes
        j.setJarByClass(Q1TransacoesBrasil.class);
        j.setMapperClass(MapQ1.class);
        j.setCombinerClass(ReduceQ1.class); // soma é associativa: o próprio reduce serve de combiner
        j.setReducerClass(ReduceQ1.class);

        // 2. tipos de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // 3. arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // 4. lança o job
        return j.waitForCompletion(true);
    }

    public static class MapQ1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text chave = new Text("Brazil");
        private final IntWritable um = new IntWritable(1);

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            Transacao t = Transacao.ler(value, con); // null = cabeçalho ou linha inválida
            if (t == null) return;

            if (t.ehBrasil()) {
                con.write(chave, um);
            }
        }
    }

    public static class ReduceQ1 extends Reducer<Text, IntWritable, Text, IntWritable> {
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
