package org.apache.kafka.common.security.fips;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;

import javax.net.ssl.SSLContext;

/** Minimal runtime guard used by the FIPS distribution. */
public final class FipsRuntime {
    private static volatile boolean initialized;

    private FipsRuntime() {
    }

    public static boolean enabled() {
        return initialized;
    }

    /** Initializes the process providers after the Enterprise startup configuration enables FIPS. */
    public static synchronized void initialize() {
        if (initialized) {
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
        try {
            SSLContext context = SSLContext.getInstance("TLS", "BCJSSE");
            context.init(null, null, SecureRandom.getInstance("DEFAULT", "BCFIPS"));
            SSLContext.setDefault(context);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Unable to initialize the FIPS default TLS context", e);
        }
        initialized = true;
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
            return "1".equals(Files.readString(Path.of("/proc/sys/crypto/fips_enabled"), StandardCharsets.US_ASCII).trim());
        } catch (IOException | SecurityException e) {
            return false;
        }
    }
}
