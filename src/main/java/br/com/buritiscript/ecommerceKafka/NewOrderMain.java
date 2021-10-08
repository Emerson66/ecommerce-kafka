package br.com.buritiscript.ecommerceKafka;

import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Um cliente entrou no sistema e criou uma conta
 * ou um pedido de compra/uma nova ordem de compra
 */
public class NewOrderMain {
    public static void main(String[] args) {
        /**
         * Eu quero criar uma mensagem, enviar uma mensagem no Kafka, eu quero produzir uma mensagem
         * Para isso eu preciso criar um Producer Kafka, esse KafkaProducer precisa de dois parametros
         * de tipagem, ou seja, uma chave e um tipo da mensagem.
         */
        KafkaProducer<String, String> ;
    }
}
