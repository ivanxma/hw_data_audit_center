/** Build-time verification of the principal authentication classes on the runtime classpath. */
public class PrincipalRuntimeCheck {
    public static void main(String[] args) throws Exception {
        Class.forName("com.oracle.bmc.auth.sasl.ResourcePrincipalsLoginModule");
        Class.forName("com.oracle.bmc.auth.sasl.InstancePrincipalsLoginModule");
        Class.forName("com.oracle.bmc.auth.sasl.OciSaslClientProvider").getMethod("initialize").invoke(null);
        Class.forName("org.apache.kafka.clients.consumer.KafkaConsumer");
        if (args.length > 0 && args[0].equals("connect")) {
            // Exercise Jersey initialization, not just Kafka/SASL class loading.
            // Duplicate runtime jars previously passed the auth check but made
            // Connect exit with NoSuchMethodError before its REST API opened.
            for (String name : new String[] {
                    "org.glassfish.jersey.internal.Errors",
                    "org.glassfish.jersey.server.ApplicationHandler",
                    "org.glassfish.jersey.client.JerseyClientBuilder"}) {
                var resources = ClassLoader.getSystemResources(name.replace('.', '/') + ".class");
                if (java.util.Collections.list(resources).size() != 1) {
                    throw new IllegalStateException("Conflicting Connect runtime class: " + name);
                }
            }
            Class.forName("org.glassfish.jersey.server.ApplicationHandler").getConstructor().newInstance();
            System.out.println("Kafka Connect Jersey runtime ready");
            var folder = java.nio.file.Path.of("/kafka/connect/debezium-connector-mysql");
            try (var files = java.nio.file.Files.list(folder)) {
                var urls = files.filter(p -> p.toString().endsWith(".jar"))
                    .map(p -> { try { return p.toUri().toURL(); } catch (Exception e) { throw new RuntimeException(e); } })
                    .toArray(java.net.URL[]::new);
                try (var loader = new java.net.URLClassLoader(urls, ClassLoader.getSystemClassLoader())) {
                    Class.forName("io.debezium.storage.jdbc.history.JdbcSchemaHistory", true, loader).getConstructor().newInstance();
                    Class.forName("com.mysql.cj.jdbc.Driver", true, loader).getConstructor().newInstance();
                }
            }
            System.out.println("Debezium JDBC schema history and MySQL driver ready");
        }
        System.out.println("OCI principal Kafka runtime ready");
    }
}
