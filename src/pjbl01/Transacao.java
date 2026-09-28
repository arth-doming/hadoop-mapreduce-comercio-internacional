package pjbl01;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Representa UMA linha do dataset (uma transação comercial).
 *
 * Formato da linha (10 colunas separadas por ";"):
 *   0 country ; 1 year ; 2 commodity code ; 3 commodity ; 4 flow ;
 *   5 price (trade_usd) ; 6 weight ; 7 unit ; 8 amount ; 9 category
 *
 * Exemplo:
 *   Brazil;2016;010111;Horses, live pure-bred breeding;Export;4493103;157195;Number of items;371;01_live_animals
 *
 * Esta classe resolve dois requisitos do PjBL:
 *   1. RETIRAR O CABEÇALHO  -> a 1a linha começa com "country_or_area" e é descartada.
 *   2. TRATAR DADOS FALTANTES -> linhas com número errado de colunas ou com
 *      campos numéricos vazios/inválidos são descartadas (retorna null).
 *
 * Campos que podem vir vazios no dataset: weight e amount (quantity).
 * Por isso o amount é guardado como Double (objeto): null = faltante.
 * Cada Mapper verifica apenas os campos que realmente usa.
 */
public class Transacao {

    public static final String SEPARADOR = ";";
    public static final int NUM_COLUNAS = 10;

    // contadores do Hadoop: aparecem no console no fim de cada job
    public enum Contadores { CABECALHO, LINHA_INVALIDA, DADO_FALTANTE }

    private String pais;
    private int ano;
    private String codigoCommodity;
    private String commodity;
    private String fluxo;
    private double preco;
    private Double peso;      // pode ser null (faltante)
    private String unidade;
    private Double quantidade; // coluna "amount" – pode ser null (faltante)
    private String categoria;

    /** true se a linha é o cabeçalho do CSV. */
    public static boolean ehCabecalho(Text linha) {
        return linha.toString().startsWith("country_or_area");
    }

    /**
     * Converte uma linha de texto em Transacao.
     * Retorna null se a linha estiver quebrada ou se faltar um campo
     * obrigatório (país, ano, fluxo, categoria ou preço).
     */
    public static Transacao parse(Text linha) {
        // o -1 mantém colunas vazias no fim da linha (ex.: "...;;01_live_animals")
        String[] c = linha.toString().split(SEPARADOR, -1);
        if (c.length != NUM_COLUNAS) {
            return null;
        }
        Transacao t = new Transacao();
        try {
            t.pais = c[0].trim();
            t.ano = Integer.parseInt(c[1].trim());
            t.codigoCommodity = c[2].trim();
            t.commodity = c[3].trim();
            t.fluxo = c[4].trim();
            t.preco = Double.parseDouble(c[5].trim());
            t.peso = paraDouble(c[6]);
            t.unidade = c[7].trim();
            t.quantidade = paraDouble(c[8]);
            t.categoria = c[9].trim();
        } catch (NumberFormatException e) {
            return null; // ano ou preço vazio/inválido
        }
        if (t.pais.isEmpty() || t.fluxo.isEmpty() || t.categoria.isEmpty()) {
            return null;
        }
        return t;
    }

    /**
     * Atalho usado por todos os Mappers: descarta o cabeçalho e as linhas
     * inválidas, contando cada caso nos contadores do Hadoop.
     * Retorna null quando a linha deve ser ignorada.
     */
    public static Transacao ler(Text linha, TaskAttemptContext con) {
        if (ehCabecalho(linha)) {
            con.getCounter(Contadores.CABECALHO).increment(1);
            return null;
        }
        Transacao t = parse(linha);
        if (t == null) {
            con.getCounter(Contadores.LINHA_INVALIDA).increment(1);
        }
        return t;
    }

    /** Campo numérico opcional: vazio -> null. */
    private static Double paraDouble(String s) {
        s = s.trim();
        if (s.isEmpty()) {
            return null;
        }
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Formata números sem notação científica (ex.: 1.2E10 -> 12000000000). */
    public static String formatar(double v) {
        if (v == Math.rint(v) && Math.abs(v) < 9e18) {
            return String.valueOf((long) v);
        }
        return String.format(java.util.Locale.US, "%.2f", v);
    }

    public boolean ehBrasil() {
        return "Brazil".equals(pais);
    }

    public String getPais() { return pais; }
    public int getAno() { return ano; }
    public String getCodigoCommodity() { return codigoCommodity; }
    public String getCommodity() { return commodity; }
    public String getFluxo() { return fluxo; }
    public double getPreco() { return preco; }
    public Double getPeso() { return peso; }
    public String getUnidade() { return unidade; }
    public Double getQuantidade() { return quantidade; }
    public String getCategoria() { return categoria; }
}
