package no.sysco.testing.kafka.pipeline.materializer.domain;

import no.sysco.testing.kafka.pipeline.avro.Message;

public final class MessageRepresentationTransformer {
  public MessageJsonRepresentation transform(final Message message) {
    return new MessageJsonRepresentation(
        message.getId(),
        message.getFrom(),
        message.getTo(),
        message.getText()
    );
  }
  public Message transform(final MessageJsonRepresentation messageJsonRepresentation) {
    return Message.newBuilder()
        .setId(messageJsonRepresentation.getId())
        .setFrom(messageJsonRepresentation.getFrom())
        .setTo(messageJsonRepresentation.getTo())
        .setText(messageJsonRepresentation.getText())
        .build();
  }
}
