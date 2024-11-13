plugins {
    id("java")
}

group = "com.ronaldsuwandi"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.8.1")
    implementation("org.apache.kafka:kafka-streams:3.8.1")
    implementation("org.apache.kafka:kafka_2.13:3.8.1")
    implementation("com.fasterxml.jackson.core:jackson-core:2.18.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.18.1")
    implementation("org.slf4j:slf4j-api:2.0.12")
    implementation("ch.qos.logback:logback-classic:1.5.6")

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.8.1")

}

tasks.test {
    useJUnitPlatform()
}
