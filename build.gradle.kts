import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.50"
}

group = "distributed.erasure.coding"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
}

subprojects {
    apply(plugin = "java")
    dependencies {
        compile(group = "io.github.microutils", name = "kotlin-logging", version = "1.7.9")
        compile(group = "org.slf4j", name = "slf4j-simple", version = "1.7.29")
    }
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}