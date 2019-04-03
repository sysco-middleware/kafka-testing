package no.sysco.testing.kafka.pipeline.producer.interfaces.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("messages")
@Produces(MediaType.APPLICATION_JSON)
public class MessageResources {

  //private KafkaRestProducer restProducer;

  public MessageResources() { }

  @GET
  public Response healthCheck() {
    return Response.ok().build();
  }
}
