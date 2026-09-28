package pjbl01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * QUESTÃO 9 – Transação com o maior e o menor valor (coluna AMOUNT), por ano e país.
 * Requisitos: Comparable writable (AnoPaisWritable como chave composta) + Combiner.
 *
 *   Map:      ((ano, país), MaiorMenorWritable(amount, commodity, fluxo))
 *   Combiner: junta localmente
 *   Reduce:   maior e menor amount de cada (ano, país)
 *
 * DADOS FALTANTES: a coluna amount está vazia em ~305 mil linhas.
 * Essas linhas são ignoradas aqui (e contadas no contador DADO_FALTANTE),
 * senão um amount vazio viraria 0 e seria o "menor" de todos.
 */
public class Q9MaiorMenorAmountAnoPais {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        System.exit(executar(new Configuration()) ? 0 : 1);
    }

    public static boolean executar(Configuration c) throws Exception {
        Path input = Caminhos.ENTRADA;
        Path output = Caminhos.saida("q9");
        Caminhos.limparSaida(c, output);

        Job j = Job.getInstance(c, "q9-maior-menor-amount-ano-pais");

        j.setJarByClass(Q9MaiorMenorAmountAnoPais.class);
        j.setMapperClass(MapQ9.class);
        j.setCombinerClass(CombineQ9.class);
        j.setReducerClass(ReduceQ9.class);

        j.setMapOutputKeyClass(AnoPaisWritable.class);
        j.setMapOutputValueClass(MaiorMenorWritable.class);
        j.setOutputKeyClass(AnoPaisWritable.class);
        j.setOutputValueClass(MaiorMenorWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        return j.waitForCompletion(true);
    }

    public static class MapQ9 extends Mapper<LongWritable, Text, AnoPaisWritable, MaiorMenorWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            Transacao t = Transacao.ler(value, con);
            if (t == null) return;

            Double amount = t.getQuantidade();
            if (amount == null) {
                con.getCounter(Transacao.Contadores.DADO_FALTANTE).increment(1);
                return;
            }
            con.write(new AnoPaisWritable(t.getAno(), t.getPais()),
                    new MaiorMenorWritable(amount, t.getCommodity(), t.getFluxo()));
        }
    }

    public static class CombineQ9 extends Reducer<AnoPaisWritable, MaiorMenorWritable, AnoPaisWritable, MaiorMenorWritable> {
        public void reduce(AnoPaisWritable key, Iterable<MaiorMenorWritable> values, Context con)
                throws IOException, InterruptedException {
            MaiorMenorWritable resultado = null;
            for (MaiorMenorWritable v : values) {
                if (resultado == null) {
                    resultado = v.copia();
                } else {
                    resultado.juntar(v);
                }
            }
            con.write(key, resultado);
        }
    }

    public static class ReduceQ9 extends Reducer<AnoPaisWritable, MaiorMenorWritable, AnoPaisWritable, MaiorMenorWritable> {
        public void reduce(AnoPaisWritable key, Iterable<MaiorMenorWritable> values, Context con)
                throws IOException, InterruptedException {
            MaiorMenorWritable resultado = null;
            for (MaiorMenorWritable v : values) {
                if (resultado == null) {
                    resultado = v.copia();
                } else {
                    resultado.juntar(v);
                }
            }
            con.write(key, resultado);
        }
    }
}
