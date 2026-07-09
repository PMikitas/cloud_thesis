FROM maven:3.9.9-eclipse-temurin-11

WORKDIR /app

COPY pom.xml ./pom.xml
COPY src ./src
COPY catalog-session.json ./catalog-session.json

RUN mkdir -p /app/results

CMD ["mvn", "gatling:test"]
