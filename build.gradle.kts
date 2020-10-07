plugins {
    java
    kotlin("jvm") version "1.4.10"
    application
    id("com.github.johnrengelman.shadow") version "5.1.0"
}

group = "uk.gov.dwp.dataworks"

repositories {
    mavenCentral()
    jcenter()
    maven(url = "https://jitpack.io")
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("com.amazonaws:aws-java-sdk-core:1.11.701")
    implementation("com.amazonaws:aws-java-sdk-s3:1.11.701")
    implementation("com.beust:klaxon:4.0.2")
    implementation("com.github.dwp:dataworks-common-logging:0.0.5")
    implementation("org.apache.commons:commons-lang3:3.9")
    implementation("org.apache.hbase:hbase-client:1.4.13")
    implementation("org.apache.hbase:hbase-server:1.4.13")
    implementation("org.apache.httpcomponents:httpclient:4.5.9")
    implementation("org.bouncycastle:bcprov-ext-jdk15on:1.62")
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.4.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.9")

    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:2.2.0")
    testImplementation("io.kotest:kotest-assertions-core-jvm:4.2.0")
    testImplementation("io.kotest:kotest-property:4.2.0")
    testImplementation("io.kotest:kotest-runner-junit5-jvm:4.2.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.4.2")
    testImplementation("com.google.code.gson:gson:2.8.5")

}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

application {
    mainClassName = "app.load.LoadKt"
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
}

configurations.all {
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
