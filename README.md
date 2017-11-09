# Easy Kafka

Данный проект содержит набор классов, упрощающих взаимодействие с Apache Kafka:
* `ru.dokwork.easy.kafka.Kafka` - билдер клиентов к kafka.
* `ru.dokwork.easy.kafka.KafkaConsumer` - простой инструмент для чтения данных из Kafka.
* `ru.dokwork.easy.kafka.KafkaProducer` - простой инструмент для записи данных в Kafka.

## How to use

### KafkaProducer

Пример конфигурирования producer-а:

```scala
import org.apache.kafka.common.serialization.StringSerializer
import ru.dokwork.easy.kafka.Kafka
import ru.dokwork.easy.kafka.KafkaProducer

val producer: KafkaProducer[String, String] = Kafka.producer[String, String]
      .withBootstrapServers(Seq("localhost:9092"))
      .withKeySerializer(new StringSerializer())
      .withValueSerializer(new StringSerializer())
      .build
```

Для конфигурирования producer-а указание bootstrap servers и сериализаторов обязательно и 
контролируется компилятором. Если не указать один из методов (к примеру withBootstrapServers),
компилятор выдаст исключение вида:
```
Error:(29, 8) KafkaClient is not completely configured: ru.dokwork.easy.kafka.configuration.KafkaProducerConfiguration[String,String,ru.dokwork.easy.kafka.configuration.Undefined,ru.dokwork.easy.kafka.configuration.Defined,ru.dokwork.easy.kafka.configuration.Defined]
      .build
```
Отправка данных в кафку заворачивается во Future:
```scala
import org.apache.kafka.clients.producer.RecordMetadata

val result: Future[RecordMetadata] = producer.send(topic, msg)
```

### KafkaConsumer

Пример конфигурирования consumer-а:

```scala
import org.apache.kafka.common.serialization.StringDeserializer
import ru.dokwork.easy.kafka.Kafka
import ru.dokwork.easy.kafka.KafkaConsumer

val consumer: KafkaConsumer[String, String] = Kafka.consumer[String, String]
      .withBootstrapServers(Seq("localhost:9092"))
      .withGroupId("test")
      .withKeyDeserializer(new StringDeserializer())
      .withValueDeserializer(new StringDeserializer())
      .build
```

Для конфигурирования consumer-а указание bootstrap servers, десериализаторов и идентификатора
группы обязательно и контролируется компилятором. Если не указать один из методов 
(к примеру withGroupId), компилятор выдаст исключение вида:

```
Error:(36, 8) KafkaClient is not completely configured: ru.dokwork.easy.kafka.configuration.ConsumerConfiguration[String,String,ru.dokwork.easy.kafka.configuration.Defined,ru.dokwork.easy.kafka.configuration.Defined,ru.dokwork.easy.kafka.configuration.Defined,ru.dokwork.easy.kafka.configuration.Undefined,ru.dokwork.easy.kafka.KafkaConsumer[String,String]]
      .build
```

Для получения данных из Kafka с помощью KafkaConsumer, необходимо определить функцию обработки 
каждой полученной записи и передать ее в метод poll:

```scala
val polling: KafkaConsumer.Polling = consumer.poll(Seq(topic)) {record: ConsumerRecord[K, V] =>  
  Future(println(record.value)) 
}
```

Каждый вызов метода Poll приводит к созданию нового `org.apache.kafka.clients.consumer.Consumer` и
полинга его в отдельном потоке. Метод возвращает `KafkaConsumer.Polling` - реализацию двух 
интерфейсов: `Awaitable` - для возможности блокировки потока с ожиданием завершения чтения kafka, 
и `Closable` - для возможости непосредственно прекращения чтения kafka:

```scala
val polling: KafkaConsumer.Polling = consumer.poll(Seq(topic)) {record: ConsumerRecord[K, V] =>  
  Future(println(record.value)) 
}
// вешаем хук за штатное заверешение чтения кафки при завершении приложения
Runtime.getRuntime.addShutdownHook(new Thread {
  override def run() = Await.result(polling.stop(), Duration.Inf)
})
// блокируем основной поток на время чтения кафки
Await.result(polling, Duration.Inf)
```

`KafkaConsumer.Polling` будет успешно завершен только в случае вызова `close`. В случае ошибок 
внутри `KafkaConsumer.RecordHandler`или при взаимодействии с Kafka, `KafkaConsumer.Polling` будет 
завершен с соответствующим исключением:
```scala
val polling: KafkaConsumer.Polling = consumer.poll(Seq(topic)) { _ =>  
  Future.exception(new ExampleException())
}

Await.result(polling, Duration.Inf) // выбросит исключение ExampleException
```

#### KafkaConsumer commit strategies

Для `KafkaConsumer` определены три стратегии комита:
- `NotCommitStrategy` - kafka property `auto.offset.reset` устанавливается в false. Ничего не 
  комитится в Kafka;
- `AutoCommitStrategy` - kafka property `auto.offset.reset` устанавливается в true со всеми 
  вытекающими
- `CommitEveryPollStrategy` - kafka property `auto.offset.reset` устанавливается в false. После
  успешной обработки всех сообщений, полученных из Kafka при последнем poll-инге, вызывается
  `org.apache.kafka.clients.consumer.Consumer.commitSync()`

Стратегия комита может быть выбрана при построении консюмера:
```scala
val consumer: KafkaConsumer[String, String] = Kafka.utils.consumer[String, String]
      .withBootstrapServers(Seq("localhost:9092"))
      .withGroupId("test")
      ...
      .withCommitStrategy.CommitEveryPoll
      ...
      .build
```
Стратегия поумолчанию: `AutoCommitStrategy`.

#### KafkaConsumer close on shutdown

Для предотвращения ситуации, когда процесс полинга может быть прерван до/во время комита полученных
и обработанных данных из-за закрытия JVM процесса, можно добавить ShutdownHook. Чтобы не делать
этого руками, в билдере консюмера предусмотрена соответствующая опция:
```scala
val consumer: KafkaConsumer[String, String] = Kafka.consumer[String, String]
      .withBootstrapServers(Seq("localhost:9092"))
      ...
      .finalizeEveryPollWithin(30.seconds)
      ...
      .build
```