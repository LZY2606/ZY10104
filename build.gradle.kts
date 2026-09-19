import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import java.util.zip.ZipFile

plugins {
    `java-library`
    alias(libs.plugins.kotlinJvm)
    alias(libs.plugins.dokka)
    alias(libs.plugins.detekt)
    signing
    `maven-publish`
    alias(libs.plugins.nexusPublish) // https://github.com/gradle-nexus/publish-plugin
}

repositories {
    mavenCentral()
}

dependencies {
    api(libs.findbugs)
    api(libs.netty)
    api(libs.j8583)
    api(libs.slf4j.api)
    api(kotlin("stdlib-jdk8"))

    testImplementation(libs.commons.lang3)
    testImplementation(libs.assertj)
    testImplementation(libs.awaitility)
    testImplementation(libs.junit.jupiter.params)
    testImplementation(libs.mockito)
    testImplementation(libs.slf4j.simple)
    testImplementation(platform(libs.spring.bom))
    testImplementation(libs.spring.context)
    testImplementation(libs.spring.test)
    testImplementation(kotlin("test-junit5"))
    testRuntimeOnly(libs.junit.jupiter.engine)
}

group = "com.github.kpavlov.jreactive8583"
version = findProperty("version")?.toString() ?: "0.0.1-SNAPSHOT"
description = "ISO8583 Connector for Netty"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar() // Include sources JAR
}

kotlin {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_17
        progressiveMode = true
        freeCompilerArgs.addAll(
            "-Xjvm-default=all",
            "-Xjsr305=strict",
            "-Xexplicit-api=strict",
        )
    }
}

tasks.test {
    useJUnitPlatform()
    // Network-contract stages run in a fixed order via the dedicated
    // verify*Contract tasks below; keep them out of the default test task.
    exclude("com/github/kpavlov/jreactive8583/contract/**")
    testLogging {
        events =
            setOf(
                TestLogEvent.PASSED,
                TestLogEvent.SKIPPED,
                TestLogEvent.FAILED,
            )
    }
}

val verifyDependenciesResolvable =
    tasks.register("verifyDependenciesResolvable") {
        group = "verification"
        description =
            "Fails fast when dependencies cannot be resolved " +
            "(e.g. --offline with an empty cache) instead of silently passing."
        doLast {
            val runtimeArtifacts = configurations.runtimeClasspath.get().resolve()
            val testRuntimeArtifacts = configurations.testRuntimeClasspath.get().resolve()
            check(runtimeArtifacts.isNotEmpty()) { "runtimeClasspath resolved to zero artifacts" }
            check(testRuntimeArtifacts.isNotEmpty()) { "testRuntimeClasspath resolved to zero artifacts" }
            logger.lifecycle(
                "Dependency gate: resolved {} runtime and {} test-runtime artifacts.",
                runtimeArtifacts.size,
                testRuntimeArtifacts.size,
            )
        }
    }

fun registerContractTestStage(
    name: String,
    stageDescription: String,
    vararg includes: String,
    configure: Test.() -> Unit = {},
): TaskProvider<Test> =
    tasks.register<Test>(name) {
        group = "verification"
        description = stageDescription
        val testSourceSet = sourceSets.test.get()
        testClassesDirs = testSourceSet.output.classesDirs
        classpath = testSourceSet.runtimeClasspath
        useJUnitPlatform()
        // Deterministic, sequential execution: the resource audit snapshots
        // thread and allocator state and must not observe other tests running in parallel.
        systemProperty("junit.jupiter.execution.parallel.enabled", "false")
        includes.forEach { include(it) }
        testLogging {
            events =
                setOf(
                    TestLogEvent.PASSED,
                    TestLogEvent.SKIPPED,
                    TestLogEvent.FAILED,
                )
        }
        doLast {
            val xmlDir = reports.junitXml.outputLocation.get().asFile
            val executed = (xmlDir.listFiles() ?: emptyArray()).count { it.name.startsWith("TEST-") }
            if (executed == 0) {
                throw GradleException(
                    "$name executed no tests; an empty verification stage must not count as success.",
                )
            }
        }
        configure()
    }

val verifyCodecContract =
    registerContractTestStage(
        "verifyCodecContract",
        "Stage 1/5: codec and pipeline unit tests.",
        "com/github/kpavlov/jreactive8583/netty/codec/*",
        "com/github/kpavlov/jreactive8583/netty/pipeline/*",
    )

val verifyVirtualTimeContract =
    registerContractTestStage(
        "verifyVirtualTimeContract",
        "Stage 2/5: virtual-time reconnect/idle tests and deterministic fault injection.",
        "com/github/kpavlov/jreactive8583/contract/VirtualTime*",
        "com/github/kpavlov/jreactive8583/contract/FaultInjection*",
    )

val verifyLoopbackContract =
    registerContractTestStage(
        "verifyLoopbackContract",
        "Stage 3/5: real loopback integration tests on dynamically bound ports.",
        "com/github/kpavlov/jreactive8583/contract/Loopback*",
    )

val verifyResourceLeakContract =
    registerContractTestStage(
        "verifyResourceLeakContract",
        "Stage 4/5: repeated rounds asserting no thread, listener or allocator growth.",
        "com/github/kpavlov/jreactive8583/contract/ResourceLeak*",
    ) {
        systemProperty("io.netty.leakDetection.level", "PARANOID")
    }

verifyCodecContract.configure { dependsOn(verifyDependenciesResolvable) }
verifyVirtualTimeContract.configure { dependsOn(verifyCodecContract) }
verifyLoopbackContract.configure { dependsOn(verifyVirtualTimeContract) }
verifyResourceLeakContract.configure { dependsOn(verifyLoopbackContract) }

val verifyJarContract =
    tasks.register("verifyJarContract") {
        group = "verification"
        description = "Stage 5/5: validates jar contents (classes, metadata, license, version, no test artifacts)."
        dependsOn(verifyResourceLeakContract, tasks.jar)
        doLast {
            val jarFile = tasks.jar.get().archiveFile.get().asFile
            val problems = mutableListOf<String>()
            ZipFile(jarFile).use { zip ->
                val names = zip.entries().asSequence().map { it.name }.toList()

                if (names.none { it.startsWith("com/github/kpavlov/jreactive8583/") && it.endsWith(".class") }) {
                    problems += "no compiled Kotlin/Java classes found"
                }
                if (names.none { it.startsWith("META-INF/") && it.endsWith(".kotlin_module") }) {
                    problems += "Kotlin module metadata (META-INF/*.kotlin_module) is missing"
                }
                if ("com/github/kpavlov/jreactive8583/iso8583fields.properties" !in names) {
                    problems += "service metadata resource iso8583fields.properties is missing"
                }
                if ("META-INF/LICENSE" !in names) {
                    problems += "META-INF/LICENSE is missing"
                }
                val manifestEntry = zip.getEntry("META-INF/MANIFEST.MF")
                val manifest = manifestEntry?.let { zip.getInputStream(it).readBytes().toString(Charsets.UTF_8) }.orEmpty()
                if (!manifest.contains("Implementation-Version: ${project.version}")) {
                    problems += "MANIFEST.MF does not declare Implementation-Version: ${project.version}"
                }
                val forbidden =
                    names.filter {
                        it.substringAfterLast('.').lowercase() in
                            setOf("jks", "p12", "pfx", "pem", "crt", "cer", "der", "key", "log", "tmp", "bak")
                    }
                if (forbidden.isNotEmpty()) {
                    problems += "test certificates or temporary logs found in jar: $forbidden"
                }
            }
            if (problems.isNotEmpty()) {
                throw GradleException(
                    "Jar contract violated for ${jarFile.name}:\n - " + problems.joinToString("\n - "),
                )
            }
            logger.lifecycle("Jar contract verified for {}", jarFile.name)
        }
    }

tasks.register("verifyNetworkContract") {
    group = "verification"
    description =
        "Runs the network contract gate in a fixed order: " +
        "codec unit tests, virtual-time reconnect/idle tests, loopback integration tests, " +
        "resource leak checks and jar content verification."
    dependsOn(verifyJarContract)
}

val dokkaJavadocJar by tasks.registering(Jar::class) {
    dependsOn(tasks.dokkaJavadoc)
    from(tasks.dokkaJavadoc.flatMap { it.outputDirectory })
    archiveClassifier.set("javadoc")
}

tasks.withType<io.gitlab.arturbosch.detekt.Detekt>().configureEach {
    jvmTarget = "17"
}

tasks.assemble {
    dependsOn(dokkaJavadocJar)
}

tasks.jar {
    from("LICENSE") {
        into("META-INF")
    }
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
        )
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifact(dokkaJavadocJar.get())

            pom {
                name.set("ISO8583 Connector for Netty")
                description.set("ISO8583 protocol client and server Netty connectors.")
                url.set("https://github.com/kpavlov/jreactive-8583")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("kpavlov")
                        name.set("Konstantin Pavlov")
                        email.set("mail@kpavlov.me")
                        url.set("https://kpavlov.me?utm_source=jreactive8583")
                        roles.set(listOf("owner", "developer"))
                    }
                }
                scm {
                    connection.set("scm:git:git@github.com:kpavlov/jreactive-8583.git")
                    developerConnection.set("scm:git:git@github.com:kpavlov/jreactive-8583.git")
                    url.set("https://github.com/kpavlov/jreactive-8583")
                    tag.set("HEAD")
                }
                inceptionYear.set("2015")
            }
        }
    }

    repositories {
        maven {
            name = "myRepo"
            url = uri(layout.buildDirectory.dir("repo"))
        }
    }
}

nexusPublishing {
    repositories {
        sonatype()
    }
}

signing {
    // https://docs.gradle.org/current/userguide/signing_plugin.html#sec:signatory_credentials
    sign(publishing.publications["maven"])
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}
