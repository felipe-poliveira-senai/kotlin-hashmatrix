plugins {
    kotlin("jvm") version "2.0.0"
}

group = "io.senai.istic.hashmatrix"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-coroutines-core
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")

    // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-serialization-json-jvm
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json-jvm:1.7.1")

    implementation("org.mongodb:mongodb-driver-kotlin-sync:5.1.1")


}

tasks.test {
    useJUnitPlatform()
}