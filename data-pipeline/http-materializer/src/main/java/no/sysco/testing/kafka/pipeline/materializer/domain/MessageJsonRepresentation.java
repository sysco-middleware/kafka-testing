package no.sysco.testing.kafka.pipeline.materializer.domain;

public class MessageJsonRepresentation {

  private String id;
  private String from;
  private String to;
  private String text;

  public MessageJsonRepresentation() {}

  public MessageJsonRepresentation(String id, String from, String to, String text) {
    this.id = id;
    this.from = from;
    this.to = to;
    this.text = text;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getFrom() {
    return from;
  }

  public void setFrom(String from) {
    this.from = from;
  }

  public String getTo() {
    return to;
  }

  public void setTo(String to) {
    this.to = to;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  @Override
  public String toString() {
    return "MessageJsonRepresentation{"
        + "id='"
        + id
        + '\''
        + ", from='"
        + from
        + '\''
        + ", to='"
        + to
        + '\''
        + ", text='"
        + text
        + '\''
        + '}';
  }

  public String json() {
    return "{"
        + "\"id\":"
        + "\""
        + id
        + "\","
        + "\"from\":"
        + "\""
        + from
        + "\","
        + "\"to\":"
        + "\""
        + to
        + "\","
        + "\"text\":"
        + "\""
        + text
        + "\""
        + "}";
  }
}
