package no.sysco.testing.kafka.pipeline.materializer.infrastructure.service;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import no.sysco.testing.kafka.pipeline.materializer.domain.MessageJsonRepresentation;

public class DatabaseWebServiceInMemmory implements DatabaseWebService {
  private static final Logger log = Logger.getLogger(DatabaseWebServiceInMemmory.class.getName());
  private List<MessageJsonRepresentation> messages;

  public DatabaseWebServiceInMemmory() {
    this.messages = new ArrayList<>();
  }

  @Override
  public void saveMessage(MessageJsonRepresentation message) {
    messages.add(message);
    log.info("Message added:" + message);
  }

  @Override
  public List<MessageJsonRepresentation> getMessages() {
    return messages;
  }
}
