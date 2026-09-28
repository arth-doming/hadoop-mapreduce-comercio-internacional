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
import java.util.Locale;

/**
 * QUESTÃO 5 – Valor médio das transações por ano, somente no Brasil.
 * Requisito: writable customizado  ->  MediaWritable(soma, quantidade).
 *
 *   Map:    só linhas do Brasil  ->  (ano, MediaWritable(preço, 1))
 *   Reduce: soma as somas e as quantidades  ->  (ano, soma/quantidade)
 */
public class Q5MediaPorAnoBrasil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        System.exit(executar(new Configuration()) ? 0 : 1);
    }

    public static boolean executar(Configuration c) throws Exception {
        Path input = Caminhos.ENTRADA;
        Path output = Caminhos.saida("q5");
        Caminhos.limparSaida(c, output);

        Job j = Job.getInstance(c, "q5-media-por-ano-brasil");

        j.setJarByClass(Q5MediaPorAnoBrasil.class);
        j.setMapperClass(MapQ5.class);
        j.setReducerClass(ReduceQ5.class);

        // a saída do MAP é diferente da saída final, então declaramos as duas
        j.setMapOutputKeyClass(IntWritable.class);
        j.setMapOutputValueClass(MediaWritable.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        return j.waitForCompletion(true);
    }

    public static class MapQ5 extends Mapper<LongWritable, Text, IntWritable, MediaWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            Transacao t = Transacao.ler(value, con);
            if (t == null) return;

            if (t.ehBrasil()) {
                con.write(new IntWritable(t.getAno()), new MediaWritable(t.getPreco(), 1));
            }
        }
    }

    public static class ReduceQ5 extends Reducer<IntWritable, MediaWritable, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<MediaWritable> values, Context con)
                throws IOException, InterruptedException {
            double soma = 0;
            long qtd = 0;
            for (MediaWritable v : values) {
                soma += v.getSoma();
                qtd += v.getQuantidade();
            }
            double media = soma / qtd;
            // formatação só para o txt ficar legível (sem notação científica)
            con.write(key, new Text(String.format(Locale.US, "%.2f", media)));
        }
    }
}
