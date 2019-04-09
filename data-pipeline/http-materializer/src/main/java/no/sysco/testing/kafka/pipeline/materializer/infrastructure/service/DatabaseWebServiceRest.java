package no.sysco.testing.kafka.pipeline.materializer.infrastructure.service;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import no.sysco.testing.kafka.pipeline.materializer.MaterializerConfig;
import no.sysco.testing.kafka.pipeline.materializer.domain.MessageJsonRepresentation;


public class DatabaseWebServiceRest implements DatabaseWebService {
  private static final Logger log = Logger.getLogger(DatabaseWebServiceRest.class.getName());
  private final String url;
  private final Client client;

  public DatabaseWebServiceRest(final MaterializerConfig applicationConfig) {
    this.url = applicationConfig.databaseRestServiceConfig.url;
    this.client = ClientBuilder.newClient();
  }

  @Override public void saveMessage(final MessageJsonRepresentation message) {
    final Response postResponse = client.target(url)
        .request()
        .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .post(Entity.json(new MessageJsonRepresentation(
            message.getId(), message.getFrom(), message.getTo(), message.getText()
        )));

    if (postResponse.getStatus() != 201) {
      log.log(Level.SEVERE, "Response failed with status {}", postResponse.getStatus());
      throw new RuntimeException("Request failed with status "+postResponse.getStatus());
    }

  }


  @Override public List<MessageJsonRepresentation> getMessages() {
    final MessageJsonRepresentation[] body = client.target(url)
        .request()
        .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .get(MessageJsonRepresentation[].class);

    return Arrays.asList(body);
  }

}
