FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/*.jar gateway-app.jar
ENTRYPOINT ["java", "-jar", "gateway-app.jar"]
