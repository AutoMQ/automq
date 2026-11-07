package org.apache.kafka.common.security.fips;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;

/** Minimal runtime guard used by the FIPS distribution. */
public final class FipsRuntime {
    public static final String MODE_PROPERTY = "automq.fips.mode";

    private FipsRuntime() {
    }

    public static boolean enabled() {
        return "enabled".equalsIgnoreCase(System.getProperty(MODE_PROPERTY, "disabled"));
    }

    public static synchronized void initialize() {
        if (!enabled()) {
            return;
        }
        if (!isHostEnabled()) {
            throw new IllegalStateException("AutoMQ FIPS mode requires /proc/sys/crypto/fips_enabled=1");
        }
        addProvider("org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider", "BCFIPS");
        addProvider("org.bouncycastle.jsse.provider.BouncyCastleJsseProvider", "BCJSSE", "fips:BCFIPS");
        try {
            Class<?> registrar = Class.forName("org.bouncycastle.crypto.CryptoServicesRegistrar");
            registrar.getMethod("setApprovedOnlyMode", boolean.class).invoke(null, true);
            if (!((Boolean) registrar.getMethod("isInApprovedOnlyMode").invoke(null))) {
                throw new IllegalStateException("BCFIPS approved-only mode was not enabled");
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("BCFIPS provider is not available", e);
        }
    }

    private static void addProvider(String className, String name, String... args) {
        if (Security.getProvider(name) != null) {
            return;
        }
        try {
            Class<?> type = Class.forName(className);
            Provider provider = args.length == 0
                ? (Provider) type.getConstructor().newInstance()
                : (Provider) type.getConstructor(String.class).newInstance(args[0]);
            Security.addProvider(provider);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to load FIPS provider " + name, e);
        }
    }

    private static boolean isHostEnabled() {
        try {
            return "1".equals(Files.readString(Path.of("/proc/sys/crypto/fips_enabled")).trim());
        } catch (IOException | SecurityException e) {
            return false;
        }
    }
}
