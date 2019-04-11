FROM openjdk:8
COPY ./target/http-materializer-1.0-SNAPSHOT.jar .
COPY ./src/main/resources/config.yml .
CMD java -jar http-materializer-1.0-SNAPSHOT.jar server config.yml