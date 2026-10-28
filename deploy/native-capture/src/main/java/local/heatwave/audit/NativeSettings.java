package local.heatwave.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/** One flow per capture process; credentials remain in its protected runtime file. */
public final class NativeSettings {
    public static final ObjectMapper JSON = new ObjectMapper();
    public static volatile JsonNode settings;
    private NativeSettings() {}

    public static String secret(String name) throws Exception {
        Properties values = new Properties();
        try (var input = Files.newInputStream(Path.of(settings.path("credentials_file").asText()))) {
            values.load(input);
        }
        String value = values.getProperty(name);
        if (value == null) throw new IllegalStateException("Runtime credential is unavailable.");
        return value;
    }

    public static String identifier(String value) {
        if (!value.matches("[A-Za-z_][A-Za-z0-9_$]{0,63}")) {
            throw new IllegalArgumentException("Invalid state-store identifier.");
        }
        return "`" + value + "`";
    }

    public static long flowId() { return settings.path("capture_control").path("flow_id").asLong(); }
    public static String name() { return settings.path("connector_name").asText(); }
    public static String table() {
        return identifier(settings.path("capture_control").path("target").path("database").asText()) + ".audit_native_offsets";
    }
    public static Connection connection() throws Exception {
        return DriverManager.getConnection(settings.path("state_jdbc_url").asText(),
            settings.path("capture_control").path("target").path("username").asText(), secret("history_password"));
    }
}
