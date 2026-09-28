package pjbl01;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable customizado que guarda, ao mesmo tempo, a transação de MAIOR valor
 * e a de MENOR valor de um grupo (questões 6 e 9).
 *
 * Cada lado guarda: valor, commodity e fluxo — em ATRIBUTOS SEPARADOS
 * (nada de concatenar strings, como exige o enunciado).
 *
 * No Map cada transação vira um MaiorMenorWritable em que ela é, ao mesmo tempo,
 * a maior e a menor. Combiner e Reduce vão juntando (método juntar) até sobrar
 * só o maior e o menor de cada chave.
 *
 * Empate: fica a commodity (e depois o fluxo) que vem primeiro em ordem alfabética,
 * para o resultado ser sempre o mesmo, independente da ordem em que os dados chegam.
 */
public class MaiorMenorWritable implements Writable {

    private double maiorValor;
    private String maiorCommodity = "";
    private String maiorFluxo = "";

    private double menorValor;
    private String menorCommodity = "";
    private String menorFluxo = "";

    public MaiorMenorWritable() {
    }

    /** Cria a partir de UMA transação: ela é a maior e a menor ao mesmo tempo. */
    public MaiorMenorWritable(double valor, String commodity, String fluxo) {
        this.maiorValor = valor;
        this.maiorCommodity = commodity;
        this.maiorFluxo = fluxo;
        this.menorValor = valor;
        this.menorCommodity = commodity;
        this.menorFluxo = fluxo;
    }

    /** Cópia – necessária porque o Hadoop REUTILIZA o objeto do Iterable no reduce. */
    public MaiorMenorWritable copia() {
        MaiorMenorWritable m = new MaiorMenorWritable();
        m.maiorValor = maiorValor;
        m.maiorCommodity = maiorCommodity;
        m.maiorFluxo = maiorFluxo;
        m.menorValor = menorValor;
        m.menorCommodity = menorCommodity;
        m.menorFluxo = menorFluxo;
        return m;
    }

    /**
     * Desempate quando os valores são iguais: vence quem vem primeiro em ordem
     * alfabética de commodity e, se ainda empatar, de fluxo.
     */
    private static boolean venceEmpate(String commodityA, String fluxoA, String commodityB, String fluxoB) {
        int c = commodityA.compareTo(commodityB);
        return c < 0 || (c == 0 && fluxoA.compareTo(fluxoB) < 0);
    }

    /** Junta outro resultado parcial neste (usado pelo Combiner e pelo Reduce). */
    public void juntar(MaiorMenorWritable o) {
        if (o.maiorValor > maiorValor || (o.maiorValor == maiorValor
                && venceEmpate(o.maiorCommodity, o.maiorFluxo, maiorCommodity, maiorFluxo))) {
            maiorValor = o.maiorValor;
            maiorCommodity = o.maiorCommodity;
            maiorFluxo = o.maiorFluxo;
        }
        if (o.menorValor < menorValor || (o.menorValor == menorValor
                && venceEmpate(o.menorCommodity, o.menorFluxo, menorCommodity, menorFluxo))) {
            menorValor = o.menorValor;
            menorCommodity = o.menorCommodity;
            menorFluxo = o.menorFluxo;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(maiorValor);
        out.writeUTF(maiorCommodity);
        out.writeUTF(maiorFluxo);
        out.writeDouble(menorValor);
        out.writeUTF(menorCommodity);
        out.writeUTF(menorFluxo);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        maiorValor = in.readDouble();
        maiorCommodity = in.readUTF();
        maiorFluxo = in.readUTF();
        menorValor = in.readDouble();
        menorCommodity = in.readUTF();
        menorFluxo = in.readUTF();
    }

    /** Só define como o resultado aparece no arquivo txt. */
    @Override
    public String toString() {
        return "MAIOR=" + Transacao.formatar(maiorValor) + " [" + maiorCommodity + " | " + maiorFluxo + "]"
                + "\tMENOR=" + Transacao.formatar(menorValor) + " [" + menorCommodity + " | " + menorFluxo + "]";
    }

    public double getMaiorValor() { return maiorValor; }
    public void setMaiorValor(double maiorValor) { this.maiorValor = maiorValor; }
    public String getMaiorCommodity() { return maiorCommodity; }
    public void setMaiorCommodity(String maiorCommodity) { this.maiorCommodity = maiorCommodity; }
    public String getMaiorFluxo() { return maiorFluxo; }
    public void setMaiorFluxo(String maiorFluxo) { this.maiorFluxo = maiorFluxo; }
    public double getMenorValor() { return menorValor; }
    public void setMenorValor(double menorValor) { this.menorValor = menorValor; }
    public String getMenorCommodity() { return menorCommodity; }
    public void setMenorCommodity(String menorCommodity) { this.menorCommodity = menorCommodity; }
    public String getMenorFluxo() { return menorFluxo; }
    public void setMenorFluxo(String menorFluxo) { this.menorFluxo = menorFluxo; }
}
