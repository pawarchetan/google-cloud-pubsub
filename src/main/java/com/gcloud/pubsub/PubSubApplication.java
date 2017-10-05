package com.gcloud.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.gcp.pubsub.core.PubSubOperations;
import org.springframework.cloud.gcp.pubsub.support.GcpHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.annotation.IdempotentReceiver;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.gcp.pubsub.AckMode;
import org.springframework.integration.gcp.pubsub.inbound.PubSubInboundChannelAdapter;
import org.springframework.integration.gcp.pubsub.outbound.PubSubMessageHandler;
import org.springframework.integration.handler.ExpressionEvaluatingMessageProcessor;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.handler.advice.IdempotentReceiverInterceptor;
import org.springframework.integration.selector.MetadataStoreSelector;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

@SpringBootApplication
@EnableIntegration
public class PubSubApplication {

  private static final Log LOGGER = LogFactory.getLog(PubSubApplication.class);

  public static void main(String[] args) throws IOException {
    SpringApplication.run(PubSubApplication.class, args);
  }

  @Bean
  public MessageChannel pubsubInputChannel() {
    return new DirectChannel();
  }

  @Bean
  public PubSubInboundChannelAdapter messageChannelAdapter(
      @Qualifier("pubsubInputChannel") MessageChannel inputChannel,
      PubSubOperations pubSubTemplate) {
    PubSubInboundChannelAdapter adapter =
        new PubSubInboundChannelAdapter(pubSubTemplate, "subscription_name");
    adapter.setOutputChannel(inputChannel);
    adapter.setAckMode(AckMode.MANUAL);

    return adapter;
  }

  @Bean
  @ServiceActivator(inputChannel = "pubsubInputChannel")
  @IdempotentReceiver("idempotentReceiverInterceptor")
  public MessageHandler messageReceiver() {
    return message -> {
      String receivedMessage  =(String) message.getPayload();
      LOGGER.info("Message arrived! : " + receivedMessage);
      AckReplyConsumer consumer =
          (AckReplyConsumer) message.getHeaders().get(GcpHeaders.ACKNOWLEDGEMENT);
      try
      {
        LOGGER.info("****** Acknowledging Message(ACK) : " + message + " ******");
        consumer.ack();
      }
      catch (Exception e)
      {
        e.printStackTrace();
        LOGGER.info("****** Negative Acknowledgement(NACK) : " + message + " ******");
        consumer.nack();
      }

    };
  }

  @Bean
  @ServiceActivator(inputChannel = "pubsubOutputChannel")
  public MessageHandler messageSender(PubSubOperations pubsubTemplate) {
    return new PubSubMessageHandler(pubsubTemplate, "topic_name");
  }

  @MessagingGateway(defaultRequestChannel = "pubsubOutputChannel")
  public interface PubsubOutboundGateway {

    void sendToPubsub(String text);
  }

  @Bean
  public IdempotentReceiverInterceptor idempotentReceiverInterceptor() {
    ExpressionParser parser = new SpelExpressionParser();
    Expression exp = parser.parseExpression("payload");
    MetadataStoreSelector selector = new MetadataStoreSelector(new ExpressionEvaluatingMessageProcessor<>(exp));
    IdempotentReceiverInterceptor interceptor = new IdempotentReceiverInterceptor(selector);
    interceptor.setDiscardChannel(discardChannel());
    return interceptor;
  }

  @Bean
  public MessageChannel discardChannel() {
    return new DirectChannel();
  }

  @Bean
  @ServiceActivator(inputChannel = "discardChannel")
  public LoggingHandler discardLoggingHandler() {
    LoggingHandler handler = new LoggingHandler("WARN");
    handler.setLoggerName("Fail.LoggingHandler");
    handler.setExpression("'Message discarded: ' + payload");
    return handler;
  }
}
