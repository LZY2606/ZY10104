import org.gradle.api.tasks.testing.TestDescriptor
import org.gradle.api.tasks.testing.TestListener
import org.gradle.api.tasks.testing.TestResult
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.TaskAction
import java.util.jar.JarFile
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
// `findProperty("version")` resolves to Gradle's built-in project version, so read
// an explicitly provided -Pversion from the Gradle property provider instead and
// fall back to a defined snapshot version (never "unspecified").
version = providers.gradleProperty("version").getOrElse("0.0.1-SNAPSHOT")
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
    testLogging {
        events =
            setOf(
                TestLogEvent.PASSED,
                TestLogEvent.SKIPPED,
                TestLogEvent.FAILED,
            )
    }
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
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
        )
    }
    // Ship the license text alongside the classes it applies to.
    from(rootProject.layout.projectDirectory.file("LICENSE")) {
        into("META-INF")
        rename { "LICENSE" }
    }
}

/*
 * --------------------------------------------------------------------------
 * Network contract gate
 *
 * `./gradlew verifyNetworkContract` runs, in a fixed order:
 *   1. codec unit tests (length-prefix framing / encoder / decoder)
 *   2. virtual-time reconnect and idle tests (no wall clock, no sockets)
 *   3. real loopback integration tests (OS-assigned ports, real channels)
 *   4. resource leak checks (event loops, channels, buffers, listeners)
 *   5. jar content verification
 *
 * Each phase is a separate Test task with its own reports. Ordering is enforced
 * with `mustRunAfter`, and the aggregate task fails on the first broken stage.
 * Missing dependencies fail resolution (also with `--offline`); they are never
 * treated as a pass.
 * --------------------------------------------------------------------------
 */

abstract class VerifyJarContents : DefaultTask() {
    @get:InputFile
    abstract val jarFile: RegularFileProperty

    @get:Input
    abstract val expectedVersion: Property<String>

    @get:Input
    abstract val expectedTitle: Property<String>

    @TaskAction
    fun verify() {
        val file = jarFile.get().asFile
        if (!file.exists() || file.length() == 0L) {
            throw GradleException("Jar is missing or empty: ${file.path}")
        }

        val names = ZipFile(file).use { zip ->
            zip.entries().asSequence().map { it.name }.toList()
        }

        val failures = mutableListOf<String>()

        fun requireEntry(condition: Boolean, description: String) {
            if (!condition) failures += description
        }

        // Kotlin module metadata + compiled classes for both languages must be present.
        requireEntry(
            names.any { it == "META-INF/netty-iso8583.kotlin_module" },
            "missing Kotlin module metadata (META-INF/netty-iso8583.kotlin_module)",
        )
        requireEntry(
            names.any { it.endsWith(".class") },
            "jar contains no compiled .class files",
        )
        requireEntry(
            names.any { it == "com/github/kpavlov/jreactive8583/client/Iso8583Client.class" },
            "missing core Kotlin class Iso8583Client.class",
        )
        requireEntry(
            names.any { it == "com/github/kpavlov/jreactive8583/iso8583fields.properties" },
            "missing service/runtime metadata iso8583fields.properties",
        )
        requireEntry(
            names.any { it.equals("META-INF/LICENSE", ignoreCase = true) },
            "missing license (META-INF/LICENSE)",
        )
        requireEntry(
            names.contains("META-INF/MANIFEST.MF"),
            "missing META-INF/MANIFEST.MF",
        )

        val manifest = JarFile(file).use { it.manifest.mainAttributes }
        val title = manifest.getValue("Implementation-Title")
        val version = manifest.getValue("Implementation-Version")
        if (title != expectedTitle.get()) {
            failures += "manifest Implementation-Title was '$title', expected '${expectedTitle.get()}'"
        }
        if (version != expectedVersion.get()) {
            failures += "manifest Implementation-Version was '$version', expected '${expectedVersion.get()}'"
        }

        // Nothing from the test/runtime tooling must leak into the published artifact.
        val forbiddenNameFragments =
            listOf(
                "junit",
                "mockito",
                "awaitility",
                "assertj",
                "simplelogger",
                "application-test",
                "test-cert",
                "testcert",
                "dummy",
            )
        val forbiddenSuffixes =
            listOf(".log", ".tmp", ".pem", ".crt", ".cer", ".p12", ".jks")
        for (name in names) {
            val lower = name.lowercase()
            if (forbiddenNameFragments.any { lower.contains(it) }) {
                failures += "jar contains forbidden test/temporary entry: $name"
            }
            if (forbiddenSuffixes.any { lower.endsWith(it) }) {
                failures += "jar contains forbidden certificate/log/temp entry: $name"
            }
        }

        if (failures.isNotEmpty()) {
            throw GradleException(
                "Jar content verification failed for ${file.name}:\n - " +
                    failures.joinToString("\n - "),
            )
        }
        logger.lifecycle("Jar content verified: {} ({} entries)", file.name, names.size)
    }
}

val networkContractPhases =
    listOf("codec", "virtualTime", "loopback", "leak")

val networkContractTestTasks =
    networkContractPhases.map { phase ->
        tasks.register<Test>("test${phase.replaceFirstChar { it.uppercase() }}Contract") {
            group = "verification"
            description = "Network contract phase: $phase"
            testClassesDirs = sourceSets.test.get().output.classesDirs
            classpath = sourceSets.test.get().runtimeClasspath
            useJUnitPlatform {
                when (phase) {
                    "codec" -> includeTags("codec")
                    "virtualTime" -> includeTags("virtualtime")
                    "loopback" -> includeTags("loopback")
                    "leak" -> includeTags("leak")
                }
            }
            // The phases install their own attributed TrackingByteBufAllocator
            // which is the authoritative buffer-reclamation check. Netty's own
            // PARANOID detector would wrap buffers and interfere with that exact
            // accounting, so it is disabled (not silently ignored).
            systemProperty("io.netty.leakDetection.level", "DISABLED")
            testLogging {
                events =
                    setOf(
                        TestLogEvent.PASSED,
                        TestLogEvent.SKIPPED,
                        TestLogEvent.FAILED,
                    )
            }
            // Re-run every time; a cached "green" must not masquerade as verification.
            outputs.upToDateWhen { false as Boolean }

            // A phase that selected zero tests is a misconfiguration, not a pass.
            var executedTests = 0
            addTestListener(
                object : TestListener {
                    override fun beforeSuite(suite: TestDescriptor) = Unit

                    override fun afterSuite(suite: TestDescriptor, result: TestResult) = Unit

                    override fun beforeTest(testDescriptor: TestDescriptor) {
                        executedTests++
                    }

                    override fun afterTest(testDescriptor: TestDescriptor, result: TestResult) = Unit
                },
            )
            doLast {
                if (executedTests == 0) {
                    throw GradleException(
                        "Network contract phase '$name' executed no tests; " +
                            "expected $phase tests to be present.",
                    )
                }
            }
        }
    }

networkContractTestTasks.zipWithNext().forEach { (previous, current) ->
    current.configure { mustRunAfter(previous) }
}

val verifyJarContents by tasks.registering(VerifyJarContents::class) {
    group = "verification"
    description = "Verifies jar classes, metadata, license and version; rejects test/temp files."
    dependsOn(tasks.named("jar"))
    jarFile.set(tasks.jar.flatMap { it.archiveFile })
    expectedTitle.set(project.name)
    expectedVersion.set(project.version.toString())
}

val verifyNetworkContract by tasks.registering {
    group = "verification"
    description = "Runs codec, virtual-time, loopback, leak and jar checks in a fixed order."
    networkContractTestTasks.forEach { dependsOn(it) }
    dependsOn(verifyJarContents)
}

// Keep jar verification last in the enforced ordering.
verifyJarContents { mustRunAfter(networkContractTestTasks.last()) }

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
