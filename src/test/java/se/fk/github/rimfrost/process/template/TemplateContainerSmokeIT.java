package se.fk.github.rimfrost.process.template;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import se.fk.rimfrost.SpecVersion;
import se.fk.rimfrost.HandlaggningRequestMessageData;
import se.fk.rimfrost.HandlaggningRequestMessagePayload;
import se.fk.rimfrost.HandlaggningResponseMessagePayload;
import se.fk.rimfrost.framework.regel.RegelRequestMessagePayload;
import se.fk.rimfrost.framework.regel.RegelRequestMessagePayloadData;
import se.fk.rimfrost.framework.regel.RegelResponseMessagePayload;
import se.fk.rimfrost.framework.regel.RegelResponseMessagePayloadData;
import se.fk.rimfrost.framework.regel.Utfall;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class TemplateContainerSmokeIT
{

   private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
         .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
   private static ConfluentKafkaContainer kafka;
   private static GenericContainer<?> process;
   private static final String kafkaImage = TestConfig.get("kafka.image");
   private static final String testImage = TestConfig.get("test.image");
   private static final int topicTimeout = TestConfig.getInt("topic.timeout");
   private static final String networkAlias = TestConfig.get("network.alias");
   private static final String smallryeKafkaBootstrapServers = networkAlias + ":9093";
   private static Network network = Network.newNetwork();

   //TODO replace these topics
   private static final String templateHandlaggningRequestTopic = TestConfig.get("template.handlaggning.requests.topic");
   private static final String templateHandlaggningResponseTopic = TestConfig.get("template.handlaggning.responses.topic");
   private static final String regelRequestTopic = TestConfig.get("template.regel.requests.topic");
   private static final String regelResponseTopic = TestConfig.get("template.regel.responses.topic");

   @BeforeAll
   static void setupKafka()
   {

      kafka = new ConfluentKafkaContainer(DockerImageName.parse(kafkaImage)
            .asCompatibleSubstituteFor("apache/kafka"))
            .withNetwork(network)
            .withNetworkAliases(networkAlias);
      kafka.start();
      try
      {
         createTopic(templateHandlaggningRequestTopic, 1, (short) 1);
         createTopic(templateHandlaggningResponseTopic, 1, (short) 1);
         createTopic(regelRequestTopic, 1, (short) 1);
         createTopic(regelResponseTopic, 1, (short) 1);
      }
      catch (Exception e)
      {
         throw new RuntimeException("Failed to create Kafka topics", e);
      }
      setupProcess();
   }

   @AfterAll
   static void tearDown()
   {
      if (process != null)
         process.stop();
      if (kafka != null)
         kafka.stop();
   }

   @Test
   void TestProcessSmoke() throws Exception
   {
      var handlaggningId = UUID.randomUUID().toString();
      var responderRegel = startKafkaResponder(regelRequestTopic, regelResponseTopic, Utfall.JA);

      // Send Handlaggning request to start workflow
      sendProcessHandlaggningRequest(handlaggningId, "A1");

      // Verify regel request message produced by process
      var regelRequest = readKafkaRequestMessage(regelRequestTopic);
      var regelRequestMessagePayload = mapper.readValue(regelRequest, RegelRequestMessagePayload.class);
      assertEquals(handlaggningId, regelRequestMessagePayload.getData().getHandlaggningId());

      // Wait for kafka responder to complete
      responderRegel.get(topicTimeout, TimeUnit.SECONDS);

      // Wait for response from process
      var processResponse = readKafkaRequestMessage(templateHandlaggningResponseTopic);
      var handlaggningRequestMessagePayload = mapper.readValue(processResponse, HandlaggningResponseMessagePayload.class);
      assertEquals(handlaggningId, handlaggningRequestMessagePayload.getData().getHandlaggningId());
      assertEquals("GODKÄND", handlaggningRequestMessagePayload.getData().getResultat());
   }

   private String readKafkaRequestMessage(String topic)
   {
      String bootstrap = kafka.getBootstrapServers().replace("PLAINTEXT://", "");
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props))
      {
         System.out.printf("New kafka consumer subscribing to topic: %s%n", topic);
         consumer.subscribe(Collections.singletonList(topic));
         ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(120));

         if (records.isEmpty())
         {
            throw new IllegalStateException("No Kafka message received on topic " + topic);
         }
         return records.iterator().next().value();
      }
   }

   private static void setupProcess()
   {
      //noinspection resource
      process = new GenericContainer<>(DockerImageName.parse(testImage))
            .withNetwork(network)
            .withEnv("MP_MESSAGING_CONNECTOR_SMALLRYE_KAFKA_BOOTSTRAP_SERVERS", smallryeKafkaBootstrapServers);
      process.start();
   }

   private static void createTopic(String topicName, int numPartitions, short replicationFactor) throws Exception
   {
      String bootstrap = kafka.getBootstrapServers().replace("PLAINTEXT://", "");
      Properties props = new Properties();
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

      try (AdminClient admin = AdminClient.create(props))
      {
         NewTopic topic = new NewTopic(topicName, numPartitions, replicationFactor);
         admin.createTopics(List.of(topic)).all().get();
         System.out.printf("Created topic: %S%n", topicName);
      }
   }

   private CompletableFuture<Void> startKafkaResponder(String requesttopic, String responseTopic, Utfall utfall)
   {
      return CompletableFuture.runAsync(() -> {
         try (KafkaConsumer<String, String> consumer = createConsumer())
         {
            consumer.subscribe(Collections.singletonList(requesttopic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            if (records.isEmpty())
            {
               throw new IllegalStateException("No Kafka message received on " + requesttopic);
            }

            // Deserialize request message into typed payload
            String message = records.iterator().next().value();
            RegelRequestMessagePayload request = mapper.readValue(message, RegelRequestMessagePayload.class);
            // Extract data safely
            RegelRequestMessagePayloadData requestData = request.getData();
            if (requestData == null)
            {
               throw new IllegalStateException("Missing data field in Kafka message: " + message);
            }
            String handlaggningId = requestData.getHandlaggningId();
            // Create typed response data object
            RegelResponseMessagePayloadData responseData = new RegelResponseMessagePayloadData();
            responseData.setHandlaggningId(handlaggningId);
            responseData.setUtfall(utfall);

            sendRegelResponse(request, responseTopic, responseData);
            System.out.printf("Sent mock Kafka response for handlaggningId=%s%n on topic %s", handlaggningId,
                  responseTopic);
         }
         catch (Exception e)
         {
            throw new RuntimeException("Kafka responder failed", e);
         }
      }, Executors.newSingleThreadExecutor());
   }

   private void sendProcessHandlaggningRequest(String handlaggningId, String messageKey) throws Exception
   {
      HandlaggningRequestMessagePayload payload = new HandlaggningRequestMessagePayload();
      HandlaggningRequestMessageData data = new HandlaggningRequestMessageData();
      data.setHandlaggningId(handlaggningId);
      payload.setSpecversion(SpecVersion.NUMBER_1_DOT_0);
      payload.setId("TestId-001");
      payload.setSource("TestSource-001");
      payload.setType(templateHandlaggningRequestTopic);
      payload.setData(data);
      // Serialize entire payload to JSON
      String eventJson = mapper.writeValueAsString(payload);

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props))
      {
         ProducerRecord<String, String> record = new ProducerRecord<>(
               templateHandlaggningRequestTopic,
               messageKey,
               eventJson);
         System.out.printf("Kafka sending to topic : %s, json: %s%n", templateHandlaggningRequestTopic, eventJson);
         producer.send(record).get();
      }
   }

   private void sendRegelResponse(RegelRequestMessagePayload request,
         String topic,
         RegelResponseMessagePayloadData messageData) throws Exception
   {
      RegelResponseMessagePayload payload = new RegelResponseMessagePayload();
      payload.setSpecversion(request.getSpecversion());
      payload.setId(request.getId());
      payload.setSource(request.getSource());
      payload.setType(topic);
      payload.setTime(OffsetDateTime.now());
      payload.setKogitoparentprociid(request.getKogitoparentprociid());
      payload.setKogitorootprocid(request.getKogitorootprocid());
      payload.setKogitoproctype(request.getKogitoproctype());
      payload.setKogitoprocinstanceid(request.getKogitoprocinstanceid());
      payload.setKogitoprocist(request.getKogitoprocist());
      payload.setKogitoprocversion(request.getKogitoprocversion());
      payload.setKogitorootprociid(request.getKogitorootprociid());
      payload.setKogitoprocid(request.getKogitoprocid());
      payload.setKogitoprocrefid(request.getKogitoprocinstanceid());

      payload.setData(messageData);

      // Serialize entire payload to JSON
      String eventJson = mapper.writeValueAsString(payload);

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props))
      {
         ProducerRecord<String, String> record = new ProducerRecord<>(
               topic,
               request.getId(), // message key
               eventJson);
         System.out.printf("Kafka mock sending: %s\n", eventJson);
         producer.send(record).get();
      }
   }

   private KafkaConsumer<String, String> createConsumer()
   {
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      return new KafkaConsumer<>(props);
   }

}
