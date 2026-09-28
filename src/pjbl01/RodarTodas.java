package pjbl01;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Roda as 9 questões em sequência e copia o resultado de cada uma
 * (output/qN/part-r-00000) para  resultados/QN.txt  – o arquivo txt pedido na entrega.
 *
 * Cada questão também pode ser executada sozinha (cada classe tem seu próprio main).
 */
public class RodarTodas {

    interface Questao {
        boolean executar(Configuration c) throws Exception;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.WARN); // menos log no console

        Object[][] questoes = {
                {"q1", (Questao) Q1TransacoesBrasil::executar},
                {"q2", (Questao) Q2TransacoesPorAno::executar},
                {"q3", (Questao) Q3TransacoesPorCategoria::executar},
                {"q4", (Questao) Q4TransacoesPorFluxo::executar},
                {"q5", (Questao) Q5MediaPorAnoBrasil::executar},
                {"q6", (Questao) Q6MaisCaraMaisBarataBrasil2016::executar},
                {"q7", (Questao) Q7MediaExportacaoBrasil::executar},
                {"q8", (Questao) Q8MaximoPorAnoBrasilOrdenado::executar},
                {"q9", (Questao) Q9MaiorMenorAmountAnoPais::executar},
        };

        new File("resultados").mkdirs();
        for (Object[] q : questoes) {
            String nome = (String) q[0];
            long inicio = System.currentTimeMillis();
            System.out.println(">>> Rodando " + nome.toUpperCase() + " ...");

            boolean ok = ((Questao) q[1]).executar(new Configuration());
            if (!ok) {
                System.out.println("!!! " + nome + " FALHOU");
                System.exit(1);
            }

            Path origem = Paths.get("output", nome, "part-r-00000");
            Path destino = Paths.get("resultados", nome.toUpperCase() + ".txt");
            Files.copy(origem, destino, StandardCopyOption.REPLACE_EXISTING);
            System.out.printf(">>> %s ok em %.1fs -> %s%n", nome.toUpperCase(),
                    (System.currentTimeMillis() - inicio) / 1000.0, destino);
        }
        System.out.println(">>> Tudo pronto! Veja a pasta resultados/");
    }
}
