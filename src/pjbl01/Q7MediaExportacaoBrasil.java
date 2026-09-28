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
 * QUESTÃO 7 – Valor médio das transações por ano, só EXPORTAÇÕES (flow = "Export") do Brasil.
 * Requisitos: Combiner + writable customizado (MediaWritable).
 *
 *   Map:      Brasil + Export  ->  (ano, MediaWritable(preço, 1))
 *   Combiner: junta localmente -> (ano, MediaWritable(soma parcial, qtd parcial))
 *   Reduce:   soma tudo        -> (ano, soma/qtd)
 *
 * IMPORTANTE: a saída do Combiner precisa ter o MESMO tipo da saída do Map
 * (IntWritable, MediaWritable), pois o Reduce pode receber dados que passaram
 * ou não pelo Combiner. Por isso o Combiner é uma classe separada do Reduce
 * (o Reduce devolve Text com a média, o Combiner devolve MediaWritable).
 *
 * Obs.: "Re-Export" NÃO entra, só "Export".
 */
public class Q7MediaExportacaoBrasil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        System.exit(executar(new Configuration()) ? 0 : 1);
    }

    public static boolean executar(Configuration c) throws Exception {
        Path input = Caminhos.ENTRADA;
        Path output = Caminhos.saida("q7");
        Caminhos.limparSaida(c, output);

        Job j = Job.getInstance(c, "q7-media-exportacao-brasil");

        j.setJarByClass(Q7MediaExportacaoBrasil.class);
        j.setMapperClass(MapQ7.class);
        j.setCombinerClass(CombineQ7.class);
        j.setReducerClass(ReduceQ7.class);

        j.setMapOutputKeyClass(IntWritable.class);
        j.setMapOutputValueClass(MediaWritable.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        return j.waitForCompletion(true);
    }

    public static class MapQ7 extends Mapper<LongWritable, Text, IntWritable, MediaWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            Transacao t = Transacao.ler(value, con);
            if (t == null) return;

            if (t.ehBrasil() && "Export".equals(t.getFluxo())) {
                con.write(new IntWritable(t.getAno()), new MediaWritable(t.getPreco(), 1));
            }
        }
    }

    public static class CombineQ7 extends Reducer<IntWritable, MediaWritable, IntWritable, MediaWritable> {
        public void reduce(IntWritable key, Iterable<MediaWritable> values, Context con)
                throws IOException, InterruptedException {
            double soma = 0;
            long qtd = 0;
            for (MediaWritable v : values) {
                soma += v.getSoma();
                qtd += v.getQuantidade();
            }
            // NÃO divide aqui! Só agrupa (soma, qtd) para diminuir o tráfego até o Reduce
            con.write(key, new MediaWritable(soma, qtd));
        }
    }

    public static class ReduceQ7 extends Reducer<IntWritable, MediaWritable, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<MediaWritable> values, Context con)
                throws IOException, InterruptedException {
            double soma = 0;
            long qtd = 0;
            for (MediaWritable v : values) {
                soma += v.getSoma();
                qtd += v.getQuantidade();
            }
            con.write(key, new Text(String.format(Locale.US, "%.2f", soma / qtd)));
        }
    }
}
