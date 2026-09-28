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
 * QUESTÃO 6 – Transação mais cara e mais barata no Brasil em 2016 (pela coluna price).
 * Requisitos: Combiner + writable customizado (MaiorMenorWritable).
 *
 *   Map:      Brasil + 2016 -> ((2016, Brazil), MaiorMenorWritable(preço, commodity, fluxo))
 *   Combiner: junta localmente, sobra 1 maior e 1 menor por bloco do arquivo
 *   Reduce:   junta os parciais -> maior e menor finais
 *
 * Aqui o Combiner e o Reduce fazem a mesma coisa e têm a mesma saída
 * (AnoPaisWritable, MaiorMenorWritable), então a lógica é igual.
 */
public class Q6MaisCaraMaisBarataBrasil2016 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        System.exit(executar(new Configuration()) ? 0 : 1);
    }

    public static boolean executar(Configuration c) throws Exception {
        Path input = Caminhos.ENTRADA;
        Path output = Caminhos.saida("q6");
        Caminhos.limparSaida(c, output);

        Job j = Job.getInstance(c, "q6-mais-cara-mais-barata-brasil-2016");

        j.setJarByClass(Q6MaisCaraMaisBarataBrasil2016.class);
        j.setMapperClass(MapQ6.class);
        j.setCombinerClass(CombineQ6.class);
        j.setReducerClass(ReduceQ6.class);

        j.setMapOutputKeyClass(AnoPaisWritable.class);
        j.setMapOutputValueClass(MaiorMenorWritable.class);
        j.setOutputKeyClass(AnoPaisWritable.class);
        j.setOutputValueClass(MaiorMenorWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        return j.waitForCompletion(true);
    }

    public static class MapQ6 extends Mapper<LongWritable, Text, AnoPaisWritable, MaiorMenorWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            Transacao t = Transacao.ler(value, con);
            if (t == null) return;

            if (t.ehBrasil() && t.getAno() == 2016) {
                con.write(new AnoPaisWritable(t.getAno(), t.getPais()),
                        new MaiorMenorWritable(t.getPreco(), t.getCommodity(), t.getFluxo()));
            }
        }
    }

    public static class CombineQ6 extends Reducer<AnoPaisWritable, MaiorMenorWritable, AnoPaisWritable, MaiorMenorWritable> {
        public void reduce(AnoPaisWritable key, Iterable<MaiorMenorWritable> values, Context con)
                throws IOException, InterruptedException {
            MaiorMenorWritable resultado = null;
            for (MaiorMenorWritable v : values) {
                if (resultado == null) {
                    resultado = v.copia(); // copia: o Hadoop reutiliza o objeto "v"
                } else {
                    resultado.juntar(v);
                }
            }
            con.write(key, resultado);
        }
    }

    public static class ReduceQ6 extends Reducer<AnoPaisWritable, MaiorMenorWritable, AnoPaisWritable, MaiorMenorWritable> {
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
