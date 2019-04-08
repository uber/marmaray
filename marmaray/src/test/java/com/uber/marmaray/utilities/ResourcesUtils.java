package com.uber.marmaray.utilities;
import org.hibernate.validator.constraints.NotEmpty;
import java.io.File;

public class ResourcesUtils {
    public static String getTextFromResource(@NotEmpty final String fileName) throws Exception {
        return new String(getBytesFromResource(fileName));
    }

    public static byte[] getBytesFromResource(@NotEmpty final String fileName) throws Exception {
        final File file = new File(ResourcesUtils.class.getClassLoader().getResource(fileName).toURI());
        return java.nio.file.Files.readAllBytes(file.toPath());
    }
}
