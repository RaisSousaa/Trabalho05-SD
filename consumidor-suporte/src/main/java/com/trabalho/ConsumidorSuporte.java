package com.trabalho;

import com.rabbitmq.client.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import smile.classification.KNN;

import java.io.IOException;
import java.util.*;

public class ConsumidorSuporte {
    private static final String EXCHANGE_NAME = "sistema_mensagens_exchange";
    private static final String QUEUE_NAME = "fila_suporte";
    private static final String ROUTING_KEY = "support.request";
    
    private static final String DLX_NAME = "dlx_exchange";
    private static final String DLQ_NAME = "fila_erros_dlq";
    private static final String DLQ_ROUTING_KEY = "erro.suporte";

    private static final String[] CATEGORIAS_SUPORTE = {
        "Problema de acesso",
        "Problema de desempenho",
        "Problema de conexao"
    };

    private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
        "o", "a", "os", "as", "de", "do", "da", "dos", "das", "em", "no", "na", 
        "nos", "nas", "para", "por", "com", "um", "uma", "uns", "umas", "ao", 
        "aos", "e", "se", "que", "minha", "meu", "durante", "tentar", "apos", "as"
    ));

    private static KNN<double[]> modeloIA;
    private static Map<String, Integer> vocabulario = new HashMap<>();

    public static void main(String[] args) {
        System.out.println(" ========================================================");
        System.out.println(" [*] Inicializando Consumidor 2: Classificador de Suporte");
        System.out.println(" [*] Treinando modelo de Inteligencia Artificial (Smile KNN k=3)...");
        inicializarModeloIA();
        System.out.println(" [✓] Modelo de IA (KNN) treinado com sucesso!");
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

            System.out.println(" [*] Consumidor de Suporte pronto.");
            System.out.println(" [*] Fila: '" + QUEUE_NAME + "' | Routing Key: '" + ROUTING_KEY + "'");
            System.out.println(" [*] DLQ: '" + DLQ_NAME + "' via DLX '" + DLX_NAME + "' (Key: " + DLQ_ROUTING_KEY + ")");
            System.out.println(" [*] Aguardando solicitacoes de suporte tecnico...\n");

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

                    if (conteudo.contains("SIMULATED_ERROR") || conteudo.contains("CORRUPT")) {
                        throw new RuntimeException("Payload corrompido detectado na solicitacao de suporte!");
                    }

                    System.out.println("--------------------------------------------------------------------------------");
                    System.out.println(" [x] Solicitacao de Suporte Recebida:");
                    System.out.println("     ID:        " + idMsg);
                    System.out.println("     Data/Hora: " + dataHora);
                    System.out.println("     Conteudo:  \"" + conteudo + "\"");

                    String categoria = classificarProblemaComIA(conteudo);

                    System.out.printf(" [IA-2 Smile KNN] -> Categoria: %s%n", categoria.toUpperCase());

                    Thread.sleep(1200);

                    finalChannel.basicAck(deliveryTag, false);
                    System.out.println(" [ACK] Solicitacao " + idMsg + " confirmada com sucesso.");
                    System.out.println("--------------------------------------------------------------------------------\n");

                } catch (Exception e) {
                    System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    System.err.println(" [!] ERRO ao processar solicitacao (ID: " + idMsg + "): " + e.getMessage());
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

    private static void inicializarModeloIA() {
      
        List<String> textos = new ArrayList<>();
        List<Integer> rotulos = new ArrayList<>();

        adicionarExemplo(textos, rotulos, "nao consigo fazer login no portal erro credenciais invalidas", 0);
        adicionarExemplo(textos, rotulos, "esqueci minha senha email redefinicao acesso nao chega caixa postal", 0);
        adicionarExemplo(textos, rotulos, "usuario bloqueado tentativas autenticacao login conta", 0);
        adicionarExemplo(textos, rotulos, "erro 403 acesso negado entrar painel administrativo login corporativo", 0);
        adicionarExemplo(textos, rotulos, "falha autenticacao dois fatores token seguranca rejeitado login", 0);
        adicionarExemplo(textos, rotulos, "permissao insuficiente conta bloqueada trocar senha redefinir usuario login", 0);
        adicionarExemplo(textos, rotulos, "autenticacao falhou nao entra conta acesso expirado senha errada usuario", 0);
        adicionarExemplo(textos, rotulos, "recuperar senha usuario sem permissao acesso login restrito credencial", 0);
        adicionarExemplo(textos, rotulos, "falha login usuario nao autenticado credencial invalida portal", 0);

        adicionarExemplo(textos, rotulos, "lentidao excessiva carregar relatorios dashboards sistema muito lento", 1);
        adicionarExemplo(textos, rotulos, "demora varios minutos processar consultas banco dados filtros lento", 1);
        adicionarExemplo(textos, rotulos, "aplicacao congelando alto consumo memoria ram lentidao telas", 1);
        adicionarExemplo(textos, rotulos, "lentidao extrema navegacao travamentos frequentes requisicoes atraso", 1);
        adicionarExemplo(textos, rotulos, "queda severa performance horarios pico muito lento demora excessiva", 1);
        adicionarExemplo(textos, rotulos, "demorando muito travar lentidao consumo excessivo cpu demora", 1);
        adicionarExemplo(textos, rotulos, "performance ruim velocidade baixa travamento constante processamento lento", 1);
        adicionarExemplo(textos, rotulos, "dashboard demorando carregar sistema lento performance fraca atraso", 1);
        adicionarExemplo(textos, rotulos, "demora resposta travando lentidao degradacao desempenho consultas", 1);

        adicionarExemplo(textos, rotulos, "erro timeout rede tentar conectar api servidor remoto", 2);
        adicionarExemplo(textos, rotulos, "perda frequente conexao base dados central falha gateway rede", 2);
        adicionarExemplo(textos, rotulos, "conexao recusada tentar sincronizar dados host remoto nuvem", 2);
        adicionarExemplo(textos, rotulos, "falha rede servidor remoto nao responde conexao abortada abruptamente", 2);
        adicionarExemplo(textos, rotulos, "desconexao constante socket perda recorrente pacotes rede offline", 2);
        adicionarExemplo(textos, rotulos, "sem conexao comunicacao servidor caiu link fora ar host inacessivel", 2);
        adicionarExemplo(textos, rotulos, "falha comunicacao timeout socket closed network unreachable dns falhou", 2);
        adicionarExemplo(textos, rotulos, "porta fechada timeout conexao rede instavel perda pacotes internet", 2);
        adicionarExemplo(textos, rotulos, "servidor offline conexao interrompida timeout requisicao gateway rede", 2);

        for (String txt : textos) {
            String[] tokens = tokenizar(txt);
            for (String t : tokens) {
                if (!t.isEmpty() && !STOPWORDS.contains(t) && !vocabulario.containsKey(t)) {
                    vocabulario.put(t, vocabulario.size());
                }
            }
        }

        int vocabSize = vocabulario.size();
        double[][] xTrain = new double[textos.size()][vocabSize];
        int[] yTrain = new int[rotulos.size()];

        for (int i = 0; i < textos.size(); i++) {
            xTrain[i] = vetorizar(textos.get(i), vocabSize);
            yTrain[i] = rotulos.get(i);
        }

        modeloIA = KNN.fit(xTrain, yTrain, 3);
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

    private static double[] vetorizar(String texto, int vocabSize) {
        double[] vetor = new double[vocabSize];
        String[] tokens = tokenizar(texto);
        double soma = 0.0;
        for (String token : tokens) {
            if (STOPWORDS.contains(token)) {
                continue;
            }
            Integer idx = vocabulario.get(token);
            if (idx != null && idx < vocabSize) {
                vetor[idx] += 1.0;
                soma += 1.0;
            }
        }
        if (soma > 0.0) {
            double sqSum = 0.0;
            for (double v : vetor) {
                sqSum += v * v;
            }
            double denom = Math.sqrt(sqSum);
            if (denom > 0) {
                for (int i = 0; i < vetor.length; i++) {
                    vetor[i] /= denom;
                }
            }
        }
        return vetor;
    }

    private static String classificarProblemaComIA(String texto) {
        double[] vetor = vetorizar(texto, vocabulario.size());
        int predClasse = modeloIA.predict(vetor);
        if (predClasse < 0 || predClasse >= CATEGORIAS_SUPORTE.length) {
            predClasse = 0;
        }
        return CATEGORIAS_SUPORTE[predClasse];
    }
}