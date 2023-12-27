package kafka.log.streamaspect.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

public class FileUtils {
    public static String readFileToString(File file, Charset charset) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            int size;
            byte[] buffer = new byte[1024];
            while ((size = fileInputStream.read(buffer)) != -1) {
                stringBuilder.append(new String(buffer, 0, size, charset));
            }
        }
        return stringBuilder.toString();
    }

    public static void writeStringToFile(File file, String content, Charset charset) throws IOException {
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(content.getBytes(charset));
        }
    }

    public static void moveFile(File source, File target) throws IOException {
        if (target.exists()) {
            target.delete();
        }
        source.renameTo(target);
    }

    public static boolean deleteQuietly(File file) {
        if (file == null) {
            return false;
        }
        try {
            return file.delete();
        } catch (Exception e) {
            return false;
        }
    }
}
