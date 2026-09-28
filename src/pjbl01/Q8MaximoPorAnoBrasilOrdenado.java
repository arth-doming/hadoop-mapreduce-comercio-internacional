package pjbl01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * QUESTÃO 8 – Valor máximo das transações por ano no Brasil, ordenado do MAIOR para o MENOR.
 * Requisito: CONCATENAÇÃO DE JOBS (a saída do job 1 é a entrada do job 2).
 *
 * Por que 2 jobs? O Hadoop só ordena pela CHAVE. No job 1 a chave é o ano
 * (precisamos agrupar por ano para achar o máximo). Para ordenar pelo VALOR
 * máximo, o job 2 inverte: o valor vira chave, e usamos um comparador decrescente.
 *
 * JOB 1 – máximo por ano
 *   Map:     Brasil -> (ano, preço)
 *   Combiner/Reduce: (ano, máximo)
 *   Saída em SequenceFile (binário que preserva os tipos IntWritable/DoubleWritable,
 *   assim o job 2 lê direto sem precisar fazer split de texto).
 *
 * JOB 2 – ordenação decrescente
 *   Map:     (ano, máximo) -> (máximo, ano)        <- inverte chave e valor
 *   Sort:    DecrescenteComparator ordena as chaves do maior para o menor
 *   Reduce:  (máximo, [anos]) -> (ano, máximo)      <- desinverte para o txt final
 *   1 reducer só, para a ordenação ser global num único arquivo.
 */
public class Q8MaximoPorAnoBrasilOrdenado {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        System.exit(executar(new Configuration()) ? 0 : 1);
    }

    public static boolean executar(Configuration c) throws Exception {
        Path input = Caminhos.ENTRADA;
        Path intermediario = Caminhos.saida("q8_etapa1");
        Path output = Caminhos.saida("q8");
        Caminhos.limparSaida(c, intermediario);
        Caminhos.limparSaida(c, output);

        // ---------------- JOB 1: máximo por ano ----------------
        Job j1 = Job.getInstance(c, "q8-etapa1-maximo-por-ano");
        j1.setJarByClass(Q8MaximoPorAnoBrasilOrdenado.class);
        j1.setMapperClass(MapEtapa1.class);
        j1.setCombinerClass(ReduceEtapa1.class); // máximo é associativo: reduce serve de combiner
        j1.setReducerClass(ReduceEtapa1.class);

        j1.setMapOutputKeyClass(IntWritable.class);
        j1.setMapOutputValueClass(DoubleWritable.class);
        j1.setOutputKeyClass(IntWritable.class);
        j1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediario);
        j1.setOutputFormatClass(SequenceFileOutputFormat.class);

        // só roda o job 2 se o job 1 terminar com sucesso
        if (!j1.waitForCompletion(true)) {
            return false;
        }

        // ---------------- JOB 2: ordenação decrescente ----------------
        Job j2 = Job.getInstance(c, "q8-etapa2-ordenacao");
        j2.setJarByClass(Q8MaximoPorAnoBrasilOrdenado.class);
        j2.setMapperClass(MapEtapa2.class);
        j2.setReducerClass(ReduceEtapa2.class);
        j2.setSortComparatorClass(DecrescenteComparator.class);
        j2.setNumReduceTasks(1);

        j2.setMapOutputKeyClass(DoubleWritable.class);
        j2.setMapOutputValueClass(IntWritable.class);
        j2.setOutputKeyClass(IntWritable.class);
        j2.setOutputValueClass(Text.class);

        j2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(j2, intermediario); // entrada = saída do job 1
        FileOutputFormat.setOutputPath(j2, output);

        return j2.waitForCompletion(true);
    }

    // ===== JOB 1 =====
    public static class MapEtapa1 extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            Transacao t = Transacao.ler(value, con);
            if (t == null) return;

            if (t.ehBrasil()) {
                con.write(new IntWritable(t.getAno()), new DoubleWritable(t.getPreco()));
            }
        }
    }

    public static class ReduceEtapa1 extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            double maximo = Double.NEGATIVE_INFINITY;
            for (DoubleWritable v : values) {
                maximo = Math.max(maximo, v.get());
            }
            con.write(key, new DoubleWritable(maximo));
        }
    }

    // ===== JOB 2 =====
    // lendo SequenceFile, o Map já recebe (IntWritable ano, DoubleWritable máximo)
    public static class MapEtapa2 extends Mapper<IntWritable, DoubleWritable, DoubleWritable, IntWritable> {
        public void map(IntWritable ano, DoubleWritable maximo, Context con)
                throws IOException, InterruptedException {
            con.write(maximo, ano); // inverte: valor vira chave para ser ordenado
        }
    }

    public static class ReduceEtapa2 extends Reducer<DoubleWritable, IntWritable, IntWritable, Text> {
        public void reduce(DoubleWritable maximo, Iterable<IntWritable> anos, Context con)
                throws IOException, InterruptedException {
            // mais de um ano pode ter o mesmo máximo: escreve uma linha para cada
            for (IntWritable ano : anos) {
                con.write(new IntWritable(ano.get()), new Text(Transacao.formatar(maximo.get())));
            }
        }
    }

    /** Inverte a ordem natural do DoubleWritable -> ordem DECRESCENTE. */
    public static class DecrescenteComparator extends WritableComparator {
        public DecrescenteComparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable a, WritableComparable b) {
            return -a.compareTo(b);
        }
    }
}
