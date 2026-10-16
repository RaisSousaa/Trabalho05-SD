package com.trabalho;

import com.rabbitmq.client.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import smile.classification.DiscreteNaiveBayes;

import java.io.IOException;
import java.util.*;

public class ConsumidorSentimento {
    private static final String EXCHANGE_NAME = "sistema_mensagens_exchange";
    private static final String QUEUE_NAME = "fila_avaliacoes";
    private static final String ROUTING_KEY = "review.product";
    
    private static final String DLX_NAME = "dlx_exchange";
    private static final String DLQ_NAME = "fila_erros_dlq";
    private static final String DLQ_ROUTING_KEY = "erro.sentimento";

    private static final String[] CLASSES_SENTIMENTO = {"NEGATIVO", "NEUTRO", "POSITIVO"};

    private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
        "o", "a", "os", "as", "de", "do", "da", "dos", "das", "em", "no", "na", 
        "nos", "nas", "para", "por", "com", "um", "uma", "uns", "umas", "ao", 
        "aos", "e", "se", "que", "minha", "meu", "produto"
    ));

    private static DiscreteNaiveBayes modeloIA;
    private static Map<String, Integer> vocabulario = new HashMap<>();

    public static void main(String[] args) {
        System.out.println(" ========================================================");
        System.out.println(" [*] Inicializando Consumidor 1: Analisador de Sentimentos");
        System.out.println(" [*] Treinando modelo de Inteligencia Artificial (Smile Naive Bayes)...");
        inicializarModeloIA();
        System.out.println(" [✓] Modelo de IA treinado com sucesso!");
        System.out.println(" ========================================================");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("rabbitmq");
        factory.setUsername("admin");
        factory.setPassword("admin123");

        Connection connection = null;
        Channel channel = null;

        while (connection == null) {
            try {
                connection = factory.newConnection();
                channel = connection.createChannel();
                System.out.println(" [✓] Conectado ao RabbitMQ com sucesso!");
            } catch (Exception e) {
                System.out.println(" [!] Aguardando inicializacao do RabbitMQ (tentativa em 3s)...");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        try {
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            channel.exchangeDeclare(DLX_NAME, "direct");
            channel.queueDeclare(DLQ_NAME, true, false, false, null);
            channel.queueBind(DLQ_NAME, DLX_NAME, DLQ_ROUTING_KEY);

            Map<String, Object> argsFila = new HashMap<>();
            argsFila.put("x-dead-letter-exchange", DLX_NAME);
            argsFila.put("x-dead-letter-routing-key", DLQ_ROUTING_KEY);
            
            channel.queueDeclare(QUEUE_NAME, true, false, false, argsFila);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

            channel.basicQos(1);

            System.out.println(" [*] Consumidor de Sentimento pronto.");
            System.out.println(" [*] Fila: '" + QUEUE_NAME + "' | Routing Key: '" + ROUTING_KEY + "'");
            System.out.println(" [*] DLQ: '" + DLQ_NAME + "' via DLX '" + DLX_NAME + "'");
            System.out.println(" [*] Aguardando mensagens de avaliacao de produtos...\n");

            Channel finalChannel = channel;
            Gson gson = new Gson();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                String mensagemJson = new String(delivery.getBody(), "UTF-8");

                String idMsg = "desconhecido";
                try {
                    JsonObject json = gson.fromJson(mensagemJson, JsonObject.class);
                    if (json == null || !json.has("conteudo") || !json.has("id")) {
                        throw new IllegalArgumentException("Payload invalido ou campos obrigatorios ausentes.");
                    }

                    idMsg = json.get("id").getAsString();
                    String conteudo = json.get("conteudo").getAsString();
                    String dataHora = json.has("dataHora") ? json.get("dataHora").getAsString() : "N/D";

                    // Validação de erro simulado do gerador para comprovação da DLQ
                    if (conteudo.contains("SIMULATED_ERROR") || conteudo.contains("CORRUPT")) {
                        throw new RuntimeException("Payload corrompido detectado na mensagem de avaliacao!");
                    }

                    System.out.println("--------------------------------------------------------------------------------");
                    System.out.println(" [x] Avaliacao Recebida:");
                    System.out.println("     ID:        " + idMsg);
                    System.out.println("     Data/Hora: " + dataHora);
                    System.out.println("     Conteudo:  \"" + conteudo + "\"");

                    ClassificacaoResultado resultado = classificarSentimentoComIA(conteudo);

                    System.out.printf(" [IA-1 Smile NaiveBayes] -> Sentimento: %s (Confianca: %.1f%%)%n", 
                            resultado.classe, resultado.probabilidade * 100);

                    Thread.sleep(1200);

                    finalChannel.basicAck(deliveryTag, false);
                    System.out.println(" [ACK] Mensagem " + idMsg + " confirmada com sucesso.");
                    System.out.println("--------------------------------------------------------------------------------\n");

                } catch (Exception e) {
                    System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    System.err.println(" [!] ERRO ao processar mensagem (ID: " + idMsg + "): " + e.getMessage());
                    System.err.println(" [DLQ] Rejeitando via basicNack(requeue=false) -> Encaminhada para DLQ ('" + DLQ_NAME + "')");
                    System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
                    try {
                        finalChannel.basicNack(deliveryTag, false, false);
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }
            };

            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class ClassificacaoResultado {
        String classe;
        double probabilidade;

        ClassificacaoResultado(String classe, double probabilidade) {
            this.classe = classe;
            this.probabilidade = probabilidade;
        }
    }

    private static void inicializarModeloIA() {
        List<String> textos = new ArrayList<>();
        List<Integer> rotulos = new ArrayList<>();

        adicionarExemplo(textos, rotulos, "excelente superou expectativas muito satisfeito qualidade comprarei", 2);
        adicionarExemplo(textos, rotulos, "qualidade maravilhosa entrega rapida otimo custo beneficio recomendo demais", 2);
        adicionarExemplo(textos, rotulos, "fantastico funciona perfeitamente muito resistente comprarei novamente", 2);
        adicionarExemplo(textos, rotulos, "adorei compra excelente acabamento bonito muito eficiente dia dia", 2);
        adicionarExemplo(textos, rotulos, "experiencia incrivel alta qualidade otimo desempenho facil uso", 2);
        adicionarExemplo(textos, rotulos, "otimo perfeito aprovado bom demais espetacular maravilha parabens", 2);
        adicionarExemplo(textos, rotulos, "recomendo a todos muito bom confiavel atendimento excelente rapidez satisfeito", 2);

        adicionarExemplo(textos, rotulos, "pessimo veio defeito quebrou primeiro dia muito insatisfeito", 0);
        adicionarExemplo(textos, rotulos, "muito ruim qualidade horrivel totalmente diferente fotos divulgadas decepcao", 0);
        adicionarExemplo(textos, rotulos, "terrivel experiencia decepcionado compra nao recomendo a ninguem arrependido", 0);
        adicionarExemplo(textos, rotulos, "fragil danificado pessima qualidade materiais acabamento pessimo lixo", 0);
        adicionarExemplo(textos, rotulos, "totalmente insatisfeito dinheiro jogado fora defeito nao funciona quebrado", 0);
        adicionarExemplo(textos, rotulos, "horrivel pior compra que ja fiz estragado desastre perdi dinheiro", 0);
        adicionarExemplo(textos, rotulos, "nao comprem pessimo suporte defeituoso odiei fraco inutil quebrou", 0);

        adicionarExemplo(textos, rotulos, "mediano atende necessidades basicas preco cobrado razoavel", 1);
        adicionarExemplo(textos, rotulos, "chegou dentro prazo esperado comum sem nada especial nem defeitos simples", 1);
        adicionarExemplo(textos, rotulos, "razoavel funciona para que propoe materiais poderiam melhores", 1);
        adicionarExemplo(textos, rotulos, "ok nem muito bom nem muito ruim condizente valor mercado mediano", 1);
        adicionarExemplo(textos, rotulos, "dentro esperado para categoria valor acabamento aceitavel padrao comum", 1);
        adicionarExemplo(textos, rotulos, "normal simples medio ordinario sem novidades cumpre tabela basico", 1);
        adicionarExemplo(textos, rotulos, "regular intermediario funcional basico entrega prazo mediano", 1);

        for (String txt : textos) {
            String[] tokens = tokenizar(txt);
            for (String t : tokens) {
                if (!t.isEmpty() && !STOPWORDS.contains(t) && !vocabulario.containsKey(t)) {
                    vocabulario.put(t, vocabulario.size());
                }
            }
        }

        int vocabSize = vocabulario.size();
        int[][] xTrain = new int[textos.size()][vocabSize];
        int[] yTrain = new int[rotulos.size()];

        for (int i = 0; i < textos.size(); i++) {
            xTrain[i] = vetorizar(textos.get(i), vocabSize);
            yTrain[i] = rotulos.get(i);
        }

        modeloIA = new DiscreteNaiveBayes(DiscreteNaiveBayes.Model.MULTINOMIAL, 3, vocabSize);
        modeloIA.update(xTrain, yTrain);
    }

    private static void adicionarExemplo(List<String> textos, List<Integer> rotulos, String texto, int rotulo) {
        textos.add(texto);
        rotulos.add(rotulo);
    }

    private static String[] tokenizar(String texto) {
        return texto.toLowerCase()
                .replaceAll("[^a-záàâãéèêíïóôõöúçñ0-9\\s]", " ")
                .split("\\s+");
    }

    private static int[] vetorizar(String texto, int vocabSize) {
        int[] vetor = new int[vocabSize];
        String[] tokens = tokenizar(texto);
        for (String token : tokens) {
            if (STOPWORDS.contains(token)) {
                continue;
            }
            Integer idx = vocabulario.get(token);
            if (idx != null && idx < vocabSize) {
                vetor[idx]++;
            }
        }
        return vetor;
    }

    private static ClassificacaoResultado classificarSentimentoComIA(String texto) {
        int[] vetor = vetorizar(texto, vocabulario.size());
        double[] posteriors = new double[3];
        int predClasse = modeloIA.predict(vetor, posteriors);

        String nomeClasse = CLASSES_SENTIMENTO[predClasse];
        double prob = posteriors[predClasse];

        if (prob <= 0.0) {
            prob = 0.75;
        }

        return new ClassificacaoResultado(nomeClasse, prob);
    }
}