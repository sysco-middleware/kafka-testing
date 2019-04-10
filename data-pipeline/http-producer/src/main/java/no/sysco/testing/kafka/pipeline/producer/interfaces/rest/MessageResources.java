package no.sysco.testing.kafka.pipeline.producer.interfaces.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import no.sysco.testing.kafka.pipeline.producer.domain.MessageJsonRepresentation;
import no.sysco.testing.kafka.pipeline.producer.domain.MessageRepresentationTransformer;
import no.sysco.testing.kafka.pipeline.producer.infrastructure.kafka.KafkaMessageProducer;
import org.apache.avro.AvroRuntimeException;

@Path("messages")
@Produces(MediaType.APPLICATION_JSON)
public class MessageResources {
  private KafkaMessageProducer messageProducer;
  private MessageRepresentationTransformer transformer;

  public MessageResources(
      final KafkaMessageProducer messageProducer,
      final MessageRepresentationTransformer transformer) {
    this.messageProducer = messageProducer;
    this.transformer = transformer;
  }

  @GET
  public Response healthCheck() {
    return Response.ok().build();
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response produceAsync(final MessageJsonRepresentation messageJsonRepresentation) {
    try {
      messageProducer.producerMessage(transformer.transform(messageJsonRepresentation));
      return Response.accepted().build();
    } catch (AvroRuntimeException exception) {
      return Response.status(400).entity(exception.toString()).build();
    }

  }
}
