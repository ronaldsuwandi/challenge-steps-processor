plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.8.1")
    implementation("org.apache.kafka:kafka-streams:3.8.1")
    implementation("org.apache.kafka:kafka_2.13:3.8.1")

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.8.1")

}

tasks.test {
    useJUnitPlatform()
}
