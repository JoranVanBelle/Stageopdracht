FROM openjdk:17-slim

VOLUME /tmp

COPY run.sh /run.sh
RUN chmod +x /run.sh

COPY target/libs/ /libs/
COPY target/*.jar /App.jar

ENTRYPOINT [ "/run.sh" ]