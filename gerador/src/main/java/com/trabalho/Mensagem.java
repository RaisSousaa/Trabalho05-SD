package com.trabalho;

import java.util.UUID;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Mensagem {
    private String id;
    private String dataHora;
    private String tipo;
    private String conteudo;

    public Mensagem(String tipo, String conteudo) {
        // Gera um ID único automaticamente
        this.id = UUID.randomUUID().toString();
        // Formata a data e hora atual
        this.dataHora = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        this.tipo = tipo;
        this.conteudo = conteudo;
    }

    public String getId() { return id; }
    public String getDataHora() { return dataHora; }
    public String getTipo() { return tipo; }
    public String getConteudo() { return conteudo; }
}