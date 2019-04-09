package no.sysco.testing.kafka.pipeline.materializer.infrastructure.service;

import java.util.List;
import no.sysco.testing.kafka.pipeline.materializer.domain.MessageJsonRepresentation;

public interface DatabaseWebService {
  void saveMessage(MessageJsonRepresentation message);
  List<MessageJsonRepresentation> getMessages();
}
