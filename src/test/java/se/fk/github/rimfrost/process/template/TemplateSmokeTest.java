package se.fk.github.rimfrost.process.template;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class TemplateSmokeTest
{

   private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
         .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
   private static final int topicTimeout = TestConfig.getInt("topic.timeout");
   private static final Duration readTimeout = Duration.ofSeconds(120);

   //TODO replace these topics
   private static final String templateHandlaggningRequestTopic = TestConfig.get("template.handlaggning.requests.topic");
   private static final String templateHandlaggningResponseTopic = TestConfig.get("template.handlaggning.responses.topic");
   private static final String regelRequestTopic = TestConfig.get("template.regel.requests.topic");
   private static final String regelResponseTopic = TestConfig.get("template.regel.responses.topic");
   private static final String regelResponseMessageType = TestConfig.get("template.regel.responses.messageType");
   private static final String kompletteringRequestTopic = TestConfig.get("template.regel.komplettering.requests.topic");
   private static final String kompletteringResponseTopic = TestConfig.get("template.regel.komplettering.responses.topic");
   private static final String kompletteringResponseMessageType = TestConfig
         .get("template.regel.komplettering.responses.messageType");

   @ConfigProperty(name = "kafka.bootstrap.servers")
   String bootstrapServers;

   @Test
   void TestProcessSmoke() throws Exception
   {
      var handlaggningId = UUID.randomUUID().toString();
      var responderRegel = startKafkaResponder(regelRequestTopic, regelResponseTopic, regelResponseMessageType, Utfall.JA,
            handlaggningId);
      var responderKomplettering = startKafkaResponder(kompletteringRequestTopic, kompletteringResponseTopic,
            kompletteringResponseMessageType, Utfall.JA, handlaggningId);

      // Send Handlaggning request to start workflow
      sendProcessHandlaggningRequest(handlaggningId, "A1");

      // Verify komplettering request message produced by process
      var kompletteringRequest = readKafkaMessage(kompletteringRequestTopic, handlaggningId);
      var kompletteringRequestMessagePayload = mapper.readValue(kompletteringRequest, RegelRequestMessagePayload.class);
      assertEquals(handlaggningId, kompletteringRequestMessagePayload.getData().getHandlaggningId());

      // Wait for kafka responder to complete
      responderKomplettering.get(topicTimeout, TimeUnit.SECONDS);

      // Verify regel request message produced by process
      var regelRequest = readKafkaMessage(regelRequestTopic, handlaggningId);
      var regelRequestMessagePayload = mapper.readValue(regelRequest, RegelRequestMessagePayload.class);
      assertEquals(handlaggningId, regelRequestMessagePayload.getData().getHandlaggningId());

      // Wait for kafka responder to complete
      responderRegel.get(topicTimeout, TimeUnit.SECONDS);

      // Wait for response from process
      var processResponse = readKafkaMessage(templateHandlaggningResponseTopic, handlaggningId);
      var handlaggningResponseMessagePayload = mapper.readValue(processResponse, HandlaggningResponseMessagePayload.class);
      assertEquals(handlaggningId, handlaggningResponseMessagePayload.getData().getHandlaggningId());
      assertEquals("GODKÄND", handlaggningResponseMessagePayload.getData().getResultat());
   }

   /**
    * Reads the first message from {@code topic} that belongs to the given handlaggningId, skipping messages left over
    * from other tests sharing the same topics.
    */
   private String readKafkaMessage(String topic, String expectedHandlaggningId)
   {
      long deadline = System.currentTimeMillis() + readTimeout.toMillis();
      try (KafkaConsumer<String, String> consumer = createConsumer())
      {
         System.out.printf("New kafka consumer subscribing to topic: %s%n", topic);
         consumer.subscribe(Collections.singletonList(topic));
         while (System.currentTimeMillis() < deadline)
         {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (var record : records)
            {
               if (record.value().contains(expectedHandlaggningId))
               {
                  return record.value();
               }
            }
         }
         throw new IllegalStateException(
               "No Kafka message for handlaggningId " + expectedHandlaggningId + " received on topic " + topic);
      }
   }

   private CompletableFuture<Void> startKafkaResponder(String requestTopic, String responseTopic, String type, Utfall utfall,
         String expectedHandlaggningId)
   {
      return CompletableFuture.runAsync(() -> {
         long deadline = System.currentTimeMillis() + Duration.ofSeconds(topicTimeout).toMillis();
         try (KafkaConsumer<String, String> consumer = createConsumer())
         {
            consumer.subscribe(Collections.singletonList(requestTopic));
            while (System.currentTimeMillis() < deadline)
            {
               ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
               for (var record : records)
               {
                  if (!record.value().contains(expectedHandlaggningId))
                  {
                     continue;
                  }
                  RegelRequestMessagePayload request = mapper.readValue(record.value(), RegelRequestMessagePayload.class);
                  RegelRequestMessagePayloadData requestData = request.getData();
                  if (requestData == null)
                  {
                     throw new IllegalStateException("Missing data field in Kafka message: " + record.value());
                  }
                  RegelResponseMessagePayloadData responseData = new RegelResponseMessagePayloadData();
                  responseData.setHandlaggningId(requestData.getHandlaggningId());
                  responseData.setUtfall(utfall);

                  sendRegelResponse(request, responseTopic, type, responseData);
                  System.out.printf("Sent mock Kafka response for handlaggningId=%s on topic %s%n",
                        requestData.getHandlaggningId(), responseTopic);
                  return;
               }
            }
            throw new IllegalStateException(
                  "No Kafka message for handlaggningId " + expectedHandlaggningId + " received on " + requestTopic);
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
      payload.setSpecversion(SpecVersion.V1);
      payload.setId("TestId-001");
      payload.setSource("TestSource-001");
      payload.setType(templateHandlaggningRequestTopic);
      payload.setData(data);
      // Serialize entire payload to JSON
      String eventJson = mapper.writeValueAsString(payload);

      try (KafkaProducer<String, String> producer = createProducer())
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
         String type,
         RegelResponseMessagePayloadData messageData) throws Exception
   {
      RegelResponseMessagePayload payload = new RegelResponseMessagePayload();
      payload.setSpecversion(request.getSpecversion());
      payload.setId(request.getId());
      payload.setSource(request.getSource());
      payload.setType(type);
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

      try (KafkaProducer<String, String> producer = createProducer())
      {
         ProducerRecord<String, String> record = new ProducerRecord<>(
               topic,
               request.getId(), // message key
               eventJson);
         System.out.printf("Kafka mock sending: %s\n", eventJson);
         producer.send(record).get();
      }
   }

   private KafkaProducer<String, String> createProducer()
   {
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      return new KafkaProducer<>(props);
   }

   private KafkaConsumer<String, String> createConsumer()
   {
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      return new KafkaConsumer<>(props);
   }
}
