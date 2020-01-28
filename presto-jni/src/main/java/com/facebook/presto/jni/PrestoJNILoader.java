package com.facebook.presto.jni;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class PrestoJNILoader
{
    private static final Set<String> loadedLibNames = new HashSet<>();

    public static void loadJNI(String yadaLibName)
    {
        loadJNI(true, yadaLibName);
    }

    public static void loadJNI(boolean raise, String yadaLibName)
    {
        if (loadedLibNames.contains(yadaLibName)) {
            return;
        }

        synchronized (PrestoJNILoader.class) {
            try {
                ClassLoader classLoader = PrestoJNILoader.class.getClassLoader();
                URL libURL = classLoader.getResource(yadaLibName);
                extractAndLoad(libURL, yadaLibName);
                loadedLibNames.add(yadaLibName);
            }
            catch (Throwable t) {
                System.err.println("Failed to load yada-jni library: " + t.getMessage());
                System.err.println("Ensure that build was in optimized mode.");
                if (raise) {
                    throw new RuntimeException("failed to load yada-jni", t);
                }
            }
        }
    }

    private static void extractAndLoad(URL libURL, String mappedLibName)
            throws IOException
    {
        File file = File.createTempFile(PrestoJNILoader.class.getSimpleName(), mappedLibName);
        file.deleteOnExit();

        try (InputStream in = libURL.openStream();
                OutputStream out = new FileOutputStream(file)) {
            IOUtils.copy(in, out);
        }

        // TODO: verify the file, e.g. sha/md5
        System.load(file.getPath());
    }

    public static boolean isJNILoaded(String yadaLibName)
    {
        return loadedLibNames.contains(yadaLibName);
    }
}
