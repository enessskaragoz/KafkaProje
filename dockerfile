FROM openjdk:8
COPY producer.jar /producer.jar
CMD ["java", "-jar", "/producer.jar"]

FROM openjdk:8
COPY consumer.jar /consumer.jar
CMD ["java", "-jar", "/consumer.jar"]
