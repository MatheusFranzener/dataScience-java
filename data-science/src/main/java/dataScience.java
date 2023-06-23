import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.statistics.DefaultBoxAndWhiskerCategoryDataset;
import org.jfree.data.statistics.DefaultBoxAndWhiskerXYDataset;

import java.util.ArrayList;
import java.util.List;

public class dataScience {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("DataScience").master("local[*]").getOrCreate();

        String path = "C:\\Users\\matheus_hohmann\\Desktop\\data-science\\src\\main\\java\\telecom_users.csv";

        Dataset<Row> dataset = spark.read().format("csv").option("header", "true").load(path); // Lendo o arquivo excel

        dataset.show(5); // Mostrando os 5 primeiros valores
        System.out.println("Linhas: " + dataset.count()); // Mostrando a quantidade de linhas
        System.out.println("Colunas: " + dataset.columns().length); // Mostrando a quantidade de colunas
        dataset.select("TipoContrato").distinct().show(); // Mostrando os tipos de contrato
        dataset.select("TipoContrato").groupBy("TipoContrato").count().show(); // Agrupando por tipo de contrato

        // Gráfico de Histograma - Meses como cliente
        Dataset<Row> contractTypeCounts = dataset.select("MesesComoCliente").groupBy("MesesComoCliente").count().orderBy("MesesComoCliente"); // Agrupando por meses como cliente
        List<Row> rows = contractTypeCounts.collectAsList(); // Convertendo para lista

        // Criando o conjunto de dados para o histograma, fazendo com a frequência
        DefaultCategoryDataset histogramDataset = new DefaultCategoryDataset();

        for (Row row : rows) {
            histogramDataset.addValue((Number) row.get(1), "Meses Como Cliente", row.get(0).toString());
        }

        // Criando o gráfico de histograma
        JFreeChart histogramChart = ChartFactory.createBarChart(
                "Meses Como Cliente",                   // Título do gráfico
                "Meses Como Cliente",         // Rótulo do eixo X
                "Frequência",                     // Rótulo do eixo Y
                histogramDataset,                          // Conjunto de dados para o histograma
                PlotOrientation.VERTICAL,                 // Orientação do gráfico
                true, true, false);

        // Exibindo o gráfico em uma janela
        ChartFrame frame = new ChartFrame("Histograma", histogramChart);
        frame.pack();
        frame.setVisible(true);

        // Criando um gráfico de boxplot - Meses como cliente
        Dataset<Row> mesesComoClienteDataSet = dataset.select("MesesComoCliente").groupBy("MesesComoCliente").count().orderBy("MesesComoCliente");
        List<Row> rowsMesesComoCliente = mesesComoClienteDataSet.collectAsList();

        DefaultBoxAndWhiskerCategoryDataset boxPlotDataset = new DefaultBoxAndWhiskerCategoryDataset();
        List<Double> values = new ArrayList<>(); // Colocando em double para poder adicionar no boxplot

        for(Row row : rowsMesesComoCliente) {
            double value = Double.parseDouble(row.get(0).toString());
            values.add(value);
        }

        boxPlotDataset.add(values, "Meses Como Cliente", "Meses Como Cliente");

        JFreeChart boxplotChart = ChartFactory.createBoxAndWhiskerChart("Meses Como Cliente", "Meses Como Cliente", "Valor", boxPlotDataset, true);

        ChartFrame frame2 = new ChartFrame("Boxplot", boxplotChart);
        frame2.pack();
        frame2.setVisible(true);

        // Deletar a coluna de código da nossa database
        dataset = dataset.drop("Codigo");

        // Deletando todos os campos que forem nulos da dataset
        dataset = dataset.na().drop();

        // Deletando a coluna _c0
        dataset = dataset.drop("_c0");

        // Deletando todos os valores que estão duplicados
        dataset = dataset.dropDuplicates();
        dataset.show();

        // tipos de dados das colunas
        dataset.printSchema();

        // Calcular a média do tipo contrato
        Dataset<Row> mediaTipoContrato = dataset.groupBy("TipoContrato").mean(String.valueOf(Double.parseDouble("ValorMensal"))).orderBy("TipoContrato");
        mediaTipoContrato.show();

        spark.stop();
    }
}
