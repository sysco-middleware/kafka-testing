package no.sysco.testing.kafka.pipeline.producer.domain;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class MessageJsonRepresentation {

  private String id;
  private String from;
  private String to;
  private String text;

  public MessageJsonRepresentation() { }
  public MessageJsonRepresentation(String id, String from, String to, String text) {
    this.id = id;
    this.from = from;
    this.to = to;
    this.text = text;
  }

  public String getId() { return id; }
  public void setId(String id) { this.id = id; }
  public String getFrom() { return from; }
  public void setFrom(String from) { this.from = from; }
  public String getTo() { return to; }
  public void setTo(String to) { this.to = to; }
  public String getText() { return text; }
  public void setText(String text) { this.text = text; }

  @Override public String toString() {
    return "MessageJsonRepresentation{" +
        "id='" + id + '\'' +
        ", from='" + from + '\'' +
        ", to='" + to + '\'' +
        ", text='" + text + '\'' +
        '}';
  }
}
