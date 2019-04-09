package no.sysco.testing.kafka.pipeline.materializer.infrastructure.service;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;
import no.sysco.testing.kafka.pipeline.materializer.MaterializerConfig;
import no.sysco.testing.kafka.pipeline.materializer.domain.MessageJsonRepresentation;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class DatabaseWebServiceRest implements DatabaseWebService {
   private static final Logger log = Logger.getLogger(DatabaseWebServiceRest.class.getName());
   private final String url;
   private final OkHttpClient client;
   private final MediaType JSON = MediaType.get("application/json; charset=utf-8");

   public DatabaseWebServiceRest(final MaterializerConfig applicationConfig) {
    this.url = applicationConfig.databaseRestServiceConfig.url;
    this.client = new OkHttpClient();
   }

   @Override public void saveMessage(final MessageJsonRepresentation message) {
     RequestBody body = RequestBody.create(JSON, message.json());
     Request request = new Request.Builder()
         .url(url)
         .post(body)
         .build();
     try (Response response = client.newCall(request).execute()) {
       final int statusCode = response.code();
       if (statusCode != 201) throw new RuntimeException("Request failed with status "+ statusCode);
     } catch (IOException e) {
       e.printStackTrace();
       throw new RuntimeException("Request failed ", e);
     }
   }

   // ignored
   @Override public List<MessageJsonRepresentation> getMessages() { return null; }
}
