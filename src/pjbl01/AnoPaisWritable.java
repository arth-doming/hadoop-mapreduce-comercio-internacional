package pjbl01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * CHAVE COMPOSTA (ano, país) – Comparable writable exigido na questão 9
 * (também reaproveitado na questão 6 com a chave (2016, Brazil)).
 *
 * Em vez de montar a chave concatenando texto ("2016-Brazil"), o que o enunciado
 * proíbe, guardamos os dois campos separados e ensinamos o Hadoop a:
 *   - serializar  (write / readFields)
 *   - ordenar     (compareTo)  -> usado no "shuffle and sort"
 *   - particionar (hashCode)   -> decide para qual reducer vai cada chave
 *   - comparar    (equals)
 */
public class AnoPaisWritable implements WritableComparable<AnoPaisWritable> {

    private int ano;
    private String pais = "";

    public AnoPaisWritable() {
    }

    public AnoPaisWritable(int ano, String pais) {
        this.ano = ano;
        this.pais = pais;
    }

    public int getAno() { return ano; }
    public void setAno(int ano) { this.ano = ano; }
    public String getPais() { return pais; }
    public void setPais(String pais) { this.pais = pais; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(ano);
        out.writeUTF(pais);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ano = in.readInt();
        pais = in.readUTF();
    }

    /** Ordena primeiro por ano e, dentro do mesmo ano, por país (ordem alfabética). */
    @Override
    public int compareTo(AnoPaisWritable o) {
        int c = Integer.compare(ano, o.ano);
        if (c != 0) {
            return c;
        }
        return pais.compareTo(o.pais);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AnoPaisWritable)) return false;
        AnoPaisWritable that = (AnoPaisWritable) o;
        return ano == that.ano && pais.equals(that.pais);
    }

    /** Precisa ser determinístico: o HashPartitioner usa este valor. */
    @Override
    public int hashCode() {
        return Objects.hash(ano, pais);
    }

    @Override
    public String toString() {
        return ano + "\t" + pais;
    }
}
