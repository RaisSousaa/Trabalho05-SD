package com.trabalho;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.google.gson.Gson;

import java.util.Random;

public class Gerador {
    private static final String EXCHANGE_NAME = "sistema_mensagens_exchange";
    private static final String ROUTING_KEY_REVIEW = "review.product";
    private static final String ROUTING_KEY_SUPPORT = "support.request";

    private static final String[] AVALIACOES_POSITIVAS = {
        "Excelente produto, superou todas as minhas expectativas! Muito satisfeito com a qualidade.",
        "Qualidade maravilhosa, entrega rápida e ótimo custo-benefício. Recomendo demais!",
        "Produto fantástico, funciona perfeitamente e é muito resistente. Comprarei novamente.",
        "Adorei a compra! Excelente acabamento, bonito e muito eficiente no dia a dia.",
        "Experiência incrível, produto de alta qualidade com ótimo desempenho e fácil uso."
    };

    private static final String[] AVALIACOES_NEGATIVAS = {
        "Péssimo produto, veio com defeito e quebrou no primeiro dia de uso. Muito insatisfeito.",
        "Muito ruim, qualidade horrível e totalmente diferente das fotos divulgadas.",
        "Terrível experiência, decepcionado com a compra. Não recomendo a ninguém.",
        "Produto frágil e danificado, péssima qualidade de materiais e acabamento péssimo.",
        "Totalmente insatisfeito, dinheiro jogado fora, produto com defeito e não funciona."
    };

    private static final String[] AVALIACOES_NEUTRAS = {
        "Produto mediano, atende às necessidades básicas pelo preço cobrado.",
        "Chegou dentro do prazo esperado, produto comum sem nada de especial nem defeitos.",
        "Razoável, funciona para o que se propõe mas os materiais poderiam ser melhores.",
        "Produto ok, nem muito bom nem muito ruim, condizente com o valor de mercado.",
        "Dentro do esperado para essa categoria de valor, acabamento aceitável e padrão."
    };

    private static final String[] SUPORTE_ACESSO = {
        "Não consigo fazer login no portal, apresenta erro de credenciais inválidas.",
        "Esqueci minha senha e o e-mail para redefinição de acesso não chega na minha caixa postal.",
        "Usuário bloqueado após tentativas de autenticação no sistema principal.",
        "Erro 403: Acesso negado ao tentar entrar no painel administrativo com login corporativo.",
        "Falha na autenticação de dois fatores, o token de segurança está sendo rejeitado no login."
    };

    private static final String[] SUPORTE_DESEMPENHO = {
        "O sistema está com lentidão excessiva ao tentar carregar relatórios e dashboards.",
        "Demora de vários minutos para processar consultas no banco de dados e filtros.",
        "Aplicação congelando com alto consumo de memória RAM e lentidão geral nas telas.",
        "Lentidão extrema na navegação do sistema e travamentos frequentes nas requisições.",
        "Queda severa de performance durante os horários de pico, sistema fica muito lento."
    };

    private static final String[] SUPORTE_CONEXAO = {
        "Erro de timeout na rede ao tentar conectar com a API do servidor remoto.",
        "Perda frequente de conexão com a base de dados central e falha no gateway de rede.",
        "Conexão recusada ao tentar sincronizar dados com o host remoto na nuvem.",
        "Falha de rede: o servidor remoto não responde e a conexão foi abortada abruptamente.",
        "Desconexão constante do socket e perda recorrente de pacotes durante a operação."
    };

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("rabbitmq");
        factory.setUsername("admin");
        factory.setPassword("admin123");

        Connection connection = null;
        Channel channel = null;

        System.out.println(" [*] Inicializando Gerador de Mensagens...");

        while (connection == null) {
            try {
                connection = factory.newConnection();
                channel = connection.createChannel();
                System.out.println(" [✓] Conexão com RabbitMQ estabelecida com sucesso!");
            } catch (Exception e) {
                System.out.println(" [!] RabbitMQ indisponível no momento. Nova tentativa em 3 segundos...");
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

            Gson gson = new Gson();
            Random random = new Random();

            System.out.println(" [*] Gerador ativo: publicando continuamente >= 5 mensagens/segundo.");
            System.out.println(" [*] Exchange: '" + EXCHANGE_NAME + "' (topic)");
            System.out.println(" [*] Routing Keys: '" + ROUTING_KEY_REVIEW + "' e '" + ROUTING_KEY_SUPPORT + "'");

            int contador = 0;

            while (true) {
                contador++;
                String routingKey;
                String tipo;
                String conteudo;

                boolean simularErro = (contador % 25 == 0);

                if (random.nextBoolean()) {
                    routingKey = ROUTING_KEY_REVIEW;
                    tipo = "avaliacao";

                    if (simularErro) {
                        conteudo = "SIMULATED_ERROR_CORRUPT_PAYLOAD: Mensagem de avaliação com payload corrompido para DLQ.";
                    } else {
                        int categoriaSentimento = random.nextInt(3);
                        if (categoriaSentimento == 0) {
                            conteudo = AVALIACOES_POSITIVAS[random.nextInt(AVALIACOES_POSITIVAS.length)];
                        } else if (categoriaSentimento == 1) {
                            conteudo = AVALIACOES_NEGATIVAS[random.nextInt(AVALIACOES_NEGATIVAS.length)];
                        } else {
                            conteudo = AVALIACOES_NEUTRAS[random.nextInt(AVALIACOES_NEUTRAS.length)];
                        }
                    }
                } else {
                    routingKey = ROUTING_KEY_SUPPORT;
                    tipo = "suporte";

                    if (simularErro) {
                        conteudo = "SIMULATED_ERROR_CORRUPT_PAYLOAD: Mensagem de suporte técnico ilegível ou corrompida para DLQ.";
                    } else {
                        int categoriaProblema = random.nextInt(3);
                        if (categoriaProblema == 0) {
                            conteudo = SUPORTE_ACESSO[random.nextInt(SUPORTE_ACESSO.length)];
                        } else if (categoriaProblema == 1) {
                            conteudo = SUPORTE_DESEMPENHO[random.nextInt(SUPORTE_DESEMPENHO.length)];
                        } else {
                            conteudo = SUPORTE_CONEXAO[random.nextInt(SUPORTE_CONEXAO.length)];
                        }
                    }
                }

                Mensagem mensagem = new Mensagem(tipo, conteudo);
                String jsonMensagem = gson.toJson(mensagem);

                channel.basicPublish(EXCHANGE_NAME, routingKey, null, jsonMensagem.getBytes("UTF-8"));
                System.out.printf(" [x] [#%05d] Enviado '%s': [%s] %s%n", 
                        contador, routingKey, tipo.toUpperCase(), conteudo);

                Thread.sleep(190);
            }
        } catch (Exception e) {
            System.err.println(" [!] Erro inesperado no Gerador: " + e.getMessage());
            e.printStackTrace();
        }
    }
}