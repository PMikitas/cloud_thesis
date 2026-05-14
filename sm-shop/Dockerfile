FROM maven:3.9.9-eclipse-temurin-11 AS build

WORKDIR /workspace

COPY pom.xml ./
COPY sm-core-model/pom.xml sm-core-model/pom.xml
COPY sm-core-modules/pom.xml sm-core-modules/pom.xml
COPY sm-core/pom.xml sm-core/pom.xml
COPY sm-shop-model/pom.xml sm-shop-model/pom.xml
COPY sm-shop/pom.xml sm-shop/pom.xml

COPY sm-core-model/src sm-core-model/src
COPY sm-core-modules/src sm-core-modules/src
COPY sm-core/src sm-core/src
COPY sm-shop-model/src sm-shop-model/src
COPY sm-shop/src sm-shop/src

RUN mvn -pl sm-shop -am clean package -DskipTests

FROM eclipse-temurin:11-jre

WORKDIR /opt/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /files

COPY --from=build /workspace/sm-shop/target/shopizer.jar /opt/app/shopizer.jar
COPY sm-shop/files /files

EXPOSE 8081

ENV SPRING_PROFILES_ACTIVE=mysql
ENV JAVA_OPTS=""

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -Dspring.profiles.active=$SPRING_PROFILES_ACTIVE -jar /opt/app/shopizer.jar"]
