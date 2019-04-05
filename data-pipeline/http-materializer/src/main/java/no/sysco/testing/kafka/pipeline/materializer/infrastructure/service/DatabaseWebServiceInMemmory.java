package no.sysco.testing.kafka.pipeline.materializer.infrastructure.service;

import java.util.ArrayList;
import java.util.List;
import no.sysco.testing.kafka.pipeline.materializer.domain.MessageJsonRepresentation;

public class DatabaseWebServiceInMemmory implements DatabaseWebService {

  private List<MessageJsonRepresentation> messages;

  public DatabaseWebServiceInMemmory() { this.messages = new ArrayList<>(); }

  @Override public void saveMessage(MessageJsonRepresentation message) { messages.add(message); }
  public List<MessageJsonRepresentation> getMessages() { return messages; }
}
