# Third-party dependencies and licenses

Reviewed 29 October 2026 against the dependency declarations, installed Python
package metadata and upstream license files/Maven POMs. Version ranges below are
the project's declarations, not a lockfile or a list of deployed image contents.

## MySQL Connector/J

The capture build pins `com.mysql:mysql-connector-j` to **26.10.0**, replacing
9.5.0. This is the latest GA published in Maven Central at the review date and
satisfies the requested 9.7-or-later upgrade. MySQL switched from 9.7 to 26.x
version numbering. The [26.10.0 release notes](https://dev.mysql.com/doc/relnotes/connector-j/en/news-26-10-0.html)
state support for MySQL Server 8.4 and later and JDBC 4.2. Confirm source and
target server compatibility before rolling out rebuilt capture images.

The community driver is **GPL-2.0-only with Universal FOSS Exception 1.0** and
additional linking permission described in its
[versioned license file](https://github.com/mysql/mysql-connector-j/blob/26.10.0/LICENSE).
The upgrade does not convert the driver to a permissive license. The exception
has conditions; it is not a blanket permission for every proprietary application.
The repository has no application LICENSE file, so the application's licensing
and eligibility for the exception are not established by this review.

Connector/J brings in `protobuf-java` **4.36.1**, licensed BSD-3-Clause, as declared
in its [published POM](https://repo.maven.apache.org/maven2/com/mysql/mysql-connector-j/26.10.0/mysql-connector-j-26.10.0.pom).
The optional OCI dependency in that POM does not replace this project's explicit
OCI SDK version. Connector/Python is a separate dependency and retains its
existing `>=9.5,<10` range; this Java-driver upgrade does not change it.

## Declared Python runtime dependencies

| Package | Declared version | Upstream license |
| --- | --- | --- |
| [Flask](https://github.com/pallets/flask/blob/main/LICENSE.txt) | `>=3.1,<4` | BSD-3-Clause |
| [mysql-connector-python](https://github.com/mysql/mysql-connector-python/blob/trunk/LICENSE.txt) | `>=9.5,<10` | GPLv2 with FOSS exception; consult the selected distribution's terms |
| [oci](https://github.com/oracle/oci-python-sdk/blob/master/LICENSE.txt) | `>=2.160,<3` | UPL-1.0 OR Apache-2.0 |
| [gunicorn](https://github.com/benoitc/gunicorn/blob/23.0.0/LICENSE) | `>=23,<24` | MIT |
| [requests](https://github.com/psf/requests/blob/main/LICENSE) | `>=2.32,<3` | Apache-2.0 |

The capture and writer Containerfiles also install subsets of these packages
with the same ranges. Different installation dates can resolve different
versions and transitive dependencies.

## Declared Java dependencies

| Component | Version | Upstream license |
| --- | --- | --- |
| Debezium embedded engine, MySQL connector and JDBC storage | 3.4.3.Final | Apache-2.0 |
| MySQL Connector/J | 26.10.0 | GPL-2.0-only with Universal FOSS Exception 1.0 and additional permission |
| OCI Java SDK Streaming and Jersey 3 HTTP client | 3.96.0 | UPL-1.0 OR Apache-2.0 |
| SLF4J simple logger | 1.7.36 | MIT |
| JUnit (tests only) | 4.13.2 | EPL-1.0 |

Sources: [Debezium parent POM](https://repo.maven.apache.org/maven2/io/debezium/debezium-build-parent/3.4.3.Final/debezium-build-parent-3.4.3.Final.pom),
[OCI SDK parent POM](https://repo.maven.apache.org/maven2/com/oracle/oci/sdk/oci-java-sdk/3.96.0/oci-java-sdk-3.96.0.pom),
[SLF4J parent POM](https://repo.maven.apache.org/maven2/org/slf4j/slf4j-parent/1.7.36/slf4j-parent-1.7.36.pom),
[JUnit POM](https://repo.maven.apache.org/maven2/junit/junit/4.13.2/junit-4.13.2.pom).

Dependency management also selects Jersey 3.1.10, Jakarta REST API 3.1.0 and
Jackson 2.21.5. Jakarta REST declares EPL-2.0 OR GPL-2.0 with Classpath Exception;
Jackson declares Apache-2.0. Jersey contains component-specific terms: its
aggregate POM lists multiple licenses and must not be treated as one blanket
license for every included file. See the
[Jersey POM](https://repo.maven.apache.org/maven2/org/glassfish/jersey/project/3.1.10/project-3.1.10.pom),
[Jakarta REST POM](https://repo.maven.apache.org/maven2/jakarta/ws/rs/jakarta.ws.rs-api/3.1.0/jakarta.ws.rs-api-3.1.0.pom)
and [Jackson BOM](https://repo.maven.apache.org/maven2/com/fasterxml/jackson/jackson-bom/2.21.5/jackson-bom-2.21.5.pom).

H2 has been removed. Java offset-store integration tests use MySQL through
Connector/J, with no additional test database engine dependency.

## Installed Python transitive dependencies

The local review environment contains Flask 3.1.3, Connector/Python 9.7.0,
OCI SDK 2.189.0, Gunicorn 23.0.0 and Requests 2.34.2. Its transitive package
metadata reports these additional licenses:

| License | Installed packages and versions |
| --- | --- |
| MIT | asn1crypto 1.5.1; attrs 26.1.0; blinker 1.9.0; charset-normalizer 3.5.2; PyJWT 2.15.1; python-pkcs11 0.9.4; pytz 2026.5; six 1.17.0; urllib3 2.8.0 |
| MIT-0 | cffi 2.1.1 |
| BSD-3-Clause | circuitbreaker 2.1.3; click 8.5.0; idna 3.20; itsdangerous 2.2.0; Jinja2 3.1.6; MarkupSafe 3.0.4; pycparser 3.12; Werkzeug 3.1.9 |
| Apache-2.0 | aiosignal 1.4.0; frozenlist 1.8.0; multidict 7.1.0; propcache 0.5.4; pyOpenSSL 26.4.0; yarl 1.25.1 |
| Apache-2.0 AND MIT | aiohttp 3.14.5 |
| Apache-2.0 OR BSD-3-Clause | cryptography 50.0.2; python-dateutil 2.9.0.post0 |
| Apache-2.0 OR BSD-2-Clause | packaging 26.3 |
| PSF-2.0 | aiohappyeyeballs 2.7.1 |
| MPL-2.0 | certifi 2026.10.23 |
| LGPL-2.1-or-later | crc32c 2.8 |

In particular, OCI SDK 2.189.0 requires crc32c 2.8.0 on Python 3.7 and later.
This is an LGPL dependency, so the runtime is not composed solely of permissively
licensed libraries. See [crc32c's license](https://github.com/ICRAR/crc32c/blob/master/LICENSE)
and the [OCI SDK distribution metadata](https://pypi.org/pypi/oci/2.189.0/json).
The installed package's `2.8` version is equivalent to the requirement `2.8.0`.

## Review limits and distribution requirements

This is a dependency license inventory, not a legal compatibility determination
or a complete redistribution notice bundle. Python package metadata was checked
locally; the complete resolved Java transitive graph and final container OS
packages have not been inventoried. Container builds also use Python/Debian,
Maven and Eclipse Temurin/OpenJDK, and setup installs MySQL Community Server;
their own distributions include additional licenses and notices.

Before distributing an application package or image, establish its application
license, inventory the actual resolved artifacts and OS packages, retain required
copyright/license/NOTICE files, and satisfy applicable source and relinking
requirements. The capture build shades dependencies into one JAR; overlapping
license resources can be lost during shading, so the JAR alone is not evidence
that all required third-party notices have been preserved. A release-specific
notice/source bundle must cover what is actually shipped.
