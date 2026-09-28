package pjbl01;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable customizado para calcular MÉDIA (usado nas questões 5 e 7).
 *
 * Por que não mandar só o preço do Map para o Reduce?
 * Porque, com Combiner, o Reduce recebe médias parciais, e "média de médias"
 * dá resultado errado. Carregando (soma, quantidade) o cálculo fica correto:
 *     média final = soma de todas as somas / soma de todas as quantidades
 *
 * Java bean: atributos privados, getters/setters, construtor vazio.
 */
public class MediaWritable implements Writable {

    private double soma;
    private long quantidade;

    // construtor vazio: o Hadoop precisa dele para recriar o objeto (readFields)
    public MediaWritable() {
    }

    public MediaWritable(double soma, long quantidade) {
        this.soma = soma;
        this.quantidade = quantidade;
    }

    public double getSoma() { return soma; }
    public void setSoma(double soma) { this.soma = soma; }
    public long getQuantidade() { return quantidade; }
    public void setQuantidade(long quantidade) { this.quantidade = quantidade; }

    // grava os atributos (serialização) – a ORDEM tem que ser a mesma do readFields
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeLong(quantidade);
    }

    // lê os atributos na mesma ordem em que foram gravados
    @Override
    public void readFields(DataInput in) throws IOException {
        soma = in.readDouble();
        quantidade = in.readLong();
    }

    @Override
    public String toString() {
        return "soma=" + Transacao.formatar(soma) + " qtd=" + quantidade;
    }
}
