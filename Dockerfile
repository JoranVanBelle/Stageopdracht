FROM maven:3.8.7-19-jdk as build

COPY src /usr/src/app/src
COPY pom.xml /usr/src/app

RUN mvn /usr/src/app/pom.xml clean package

FROM openjdk:19-jdk

COPY /target/adapter.mvb-0.0.1-SNAPSHOT.jar adapter.jar

ENTRYPOINT ["java","-jar", "/adapter.jar"]