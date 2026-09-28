package pjbl01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Caminhos de entrada/saída usados por todas as questões.
 * O dataset deve estar em  in/operacoes_comerciais_inteira.csv  (pasta "in" do projeto).
 */
public class Caminhos {

    public static final Path ENTRADA = new Path("in/operacoes_comerciais_inteira.csv");

    /** Pasta de saída de cada questão, ex.: output/q1 */
    public static Path saida(String nome) {
        return new Path("output/" + nome);
    }

    /**
     * O Hadoop recusa rodar se a pasta de saída já existir
     * ("Output directory ... already exists"). Apagamos antes de cada execução.
     */
    public static void limparSaida(Configuration c, Path saida) throws IOException {
        FileSystem fs = FileSystem.get(c);
        if (fs.exists(saida)) {
            fs.delete(saida, true);
        }
    }
}
