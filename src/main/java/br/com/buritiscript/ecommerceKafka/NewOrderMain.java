package br.com.buritiscript.ecommerceKafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Um cliente entrou no sistema e criou uma conta
 * ou um pedido de compra/uma nova ordem de compra
 */
public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        /**
         * @STEP-01
         * Eu quero criar uma mensagem, enviar uma mensagem no Kafka, eu quero produzir uma mensagem
         * Para isso eu preciso criar um Producer Kafka, esse KafkaProducer precisa de dois parametros
         * de tipagem, ou seja, uma chave e um tipo da mensagem.
         *
         * De Inicio essa chave e tipo são Strings e a medida em que formos precisando é que discutiremos
         * os tipos.
         * > KafkaProducer<String, String>() ;
         *
         * O proximo passo é criar o producer ou produtor e usaremos o var, pois não queremos deixar o
         * código tao verboso ou redundante.
         *
         * Continuando... o KafkaProducer precisa receber uma propriedade como parametro ou melhor, ela
         * precisa receber properties. Poderiamos ler esse properties de um arquivo, mas em vez disso
         * criaremos esse properties na mão.
         *
         * Então criamos um metodo statico properties que retorna um Objeto Properties
         *
         * >var producer = new KafkaProducer<String, String>(properties()) ;
         */
        var producer = new KafkaProducer<String, String>(properties()) ;

        /**
         * @STEP-07
         * O value é a mensagem que eu quero mandar que no nosso caso vai ser respectivamente o ID do meu
         * pedido, o ID do meu usuario e o valor da compra.
         * >var value = "0034, 2140, 70.00";
         */
        var value = "0034, 2140, 70.00";

        /**
         * @STEP-06
         *Então o meu record ou o meu registro é um "registro do meu producer", logo ele é um
         * >new ProducerRecord<String, String>();
         * É importante lembrar que quando se cria um producer além de informar o IP precisamos informar o
         * tópico também. Então definimos o tópico no parâmetro de ProducerRecor como "ECOMMERCE_NEW_ORDER",
         * repare que estamos usando o mesmo padrão do nosso projeto para definir o nome do Tópico.
         * Os próximos parâmetros possui diversas variações (para ver todas as variações é só abrir o código
         * fonte de ProducerRecord), porem a que iremos utilizar é a variação "topic, key, value".
         * Por enquanto vamos mandar tanto pra chave quanto pro valor a mesma coisa e essa coisa será meu value,
         * mas quem é meu value?
         *>var record = new ProducerRecord<String,String>("ECOMMERCE_NEW_ORDEM", value, value);
         */

        /**
         * @STEP-08
         * Uma coisa interessante é que no paramêtro de tipagem do producerRecord fica meio que implicito
         * que estamos usando Strings para key e value, então não precisamos informar o tipo exato ficando
         * assim mais simples o nosso código. Agora o record está pronto para ser enviado pelo producer.
         * O próximo passo é rodar o projeto. Ele
         * >var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDEM", value, value);
         */
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDEM", value, value);

        /**
         * @STEP-05
         * Agora que eu tenho meu producer eu posso enviar alguma coisa e a maneira mais simples dissso é
         * com o >producer.send();
         * A mensagem que eu vou enviar é um record ou um registro, pois ele vai ficar registrada no kafka.
         * Para saber o quanto tempo essa mensagem vai ficar armazenada você vai depender da configuração do
         * seu serverProperties e é no serverProperties também que é definido o espaço máximo em que será
         * armazenado a mensagem. Então ou armazenamos as mensagem por tempo máximo ou armazenamos por espaço
         * máximo. No serversProperties configuramos coisas do gênero.
         * >producer.send(record);
         */

        /**
         * @STEP-10
         * Nada deve mudar, pois o método send() devolve uma Future, o Future é alguma coisa que irá executar daqui a
         * pouco (podemos ver isso abrindo o codigo fonte de send()), então o send() não é sincrono ele é assícrono,
         * então se eu quiser esperar o Future terminar devemos por um .get() no final do send(), mas isso
         * está sujeito a falhas então o proprio get possui exceções checkeds onde somos quase que obrigados
         * a fazer o método lançar, pois enquanto estamos esperando pode acontecer duas coisas:
         * - InterruptedException : equanto estamos esperando, alguem pode interromper a execuçao.
         * - ExecutionException : enquato estamos esperando, pode acontecer um erro na execução.
         */

        /**
         * @STEP-11
         * Ao executarmos nosso projeto novamente, ainda continuamos sem receber nenhuma mensagem de sucesso ou de
         * falha, pois não recebemos nenhum mensagem de sucesso ou falha. Precisamos dizer ao producer.send(record)
         * que a medida que enviamos uma mensagem nós fossemos notificados se deu sucesso ou se deu falha ou o que
         * aconteceu, para ser mais específico o que eu quero saber é que quando, em paralelo, acontecer alguma
         * coisa eu gostaria de ser notificado, então eu quero passar um Callback para o Kafka que está enviando
         * a mensagem (Kafka producer) e o send() tem uma variação que recebe um Callback que passaremos em forma de
         * lambda. Nosso Callback irá receber, os dados de sucesso ou a exception de falha, um dos dois e ela vai
         * mostrar o que aconteceu. Por exemplo, se a exception for diferente de NULL é porque deu erro, então
         * imprimimos a exception e paramos por aqui, senão a exception é NULL e deu sucesso, então imprimimos o
         * topico, a partição, o offset (a posiçaõ em que ele colocou) e o timestamp (em que momento).
         * >producer.send(record, (data, ex) -> {
         * >            if(ex != null){
         * >                ex.printStackTrace();
         * >               return;
         * >            }
         * >           System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/offset " + data.offset() + "/ timestamp " + data.timestamp());
         * >        }).get();
         */
        producer.send(record, (data, ex) -> {
            if(ex != null){
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/offset " + data.offset() + "/ timestamp " + data.timestamp());
        }).get();
    }

    private static Properties properties() {
        /**
         * @STEP-02
         * Uma maneira de facil de criar um properties é com > new Properties();
         * >var properties = new Properties();
         */
        var properties = new Properties();
        /**
         * @STEP-03
         * Agora precisamos definir algumas propriedades, para isso usa-se o setProperties(key, value)
         * A primeira propriedade a ser definida é dizer para o(s) Kafka(s) onde a gente vai se conectar,
         * pois quando rodamos um produtor precisamos informar onde que está rodando os meus Kafkas.
         * Então os meus kafkas estão rodando na chave "configuração de produdor"
         * >ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
         * e o valor será o(s) servidor(es) que nesse caso será  "127.0.0.1:9092".
         *
         * >properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
         */

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        /**
         * @STEP-04
         * A segunda propriedade importante é uma que dizemos que tanto a chave quanto o valor vão
         * transformar a mensagem e a chave, baseado em Strings, então além do tipo eu vou precisar passar
         * transformadores de Strings para bytes ou melhor, serializadores de Strings para bytes.
         *
         * Então dizemos que no producerConfig o KEY_SERIALIZER_CLASS_CONFIG é um serializador de Strings
         * >StringSerializer.class.getName();
         * e fazemos a mesma coisa para o VALUE_SERIALIZER_CLASS_CONFIG que é a mensagem.
         * Ambos vão fazer a serialização, ou seja, irão transformar Strings em bytes.
         *
         * >properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
         * >properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
         */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;

    }
}

/**
 * @STEP-09
 * Antes de rodar o projeto é importante inicializar o kafka. Para fazer isso devemos primeiro startar o
 * zookeeper com o comando >bin/zookeeper-server-start.sh config/zookeeper.properties
 * para então depois startar o kafka usando o comando
 * >bin/kafka-server-start.sh config/kafka.properties
 *Ao rodar nossa class NewOrderMain pela primeira vez ele deveria conectar no nosso kafka e tentar mandar a
 * a mensagem, mas percebemos que recebemos no console a mensagem "LEADER NOT AVAILABLE" ou seja, ele fala
 * "tentei enviar, mas não tenho nenhum lider para enviar", parece que deu alguma coisa errada, então vamos
 * ver a lista de topicos ativos usando o comando:
 * > bin/kafka-topics.sh --list --bootstrap-server localhost:9092
 * Ele deve mostrar o nosso topico ECOMMERCE_NEW_ORDER, agora pediremos para descrever todos os topicos:
 * > bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
 * Agora deve aparecer uma descrição mais detalhada do nosso topico e é nessa hora que podemos ter noção
 * do porquê que recebemos o erro, do porquê que não temos um lider.
 * A resposta é que acabamos de criar esse topico e ao rodarmos nosso projeto novamente já não vemos nenhum
 * log dizendo se foi ou não foi feito algo, então vamos rodar o --describe de novo.
 */

/**
 * @STEP-12
 * Agora eu rodo novamente o projeto e já podemos perceber que já há uma mensagem no nosso console.
 * Nela o offset deve contar como 0, pois nas outras mensagens que foram enviada nós não esperamos terminar,
 * ou seja, dessa vez eu esperei realmente a mensagem ser enviada.
 * A partir de agora toda vez que eu rodar o projeto eu quero que o offset cresça +1.
 */

/**
 * @STEP-13
 * Agora no terminal criamos um consumidor para consumir ECOMMERCE_NEW_ORDER:
 * > bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ECOMMERCE_NEW_ORDER
 * --from-beginning
 * Está pronto nosso enviador de novo pedido.
 * Resumindo: Quando o envio da nossa mensagem for sucesso eu sei que ela realmente foi enviada e quando
 * não for sucesso eu não sei se ela realmente foi.
 *
 * A questão agora é quem está escutando essa mensagem, quem são os consumidores que estão escutando essa
 * mensagem.
 */
