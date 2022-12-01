package io.trino.server;

import io.trino.server.PluginManager.PluginsProvider;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Objects.requireNonNull;

public class DynamicPluginProvider implements PluginsProvider
{
    private final String pluginName;
    private final String pluginDir;

    public DynamicPluginProvider(String pluginName, String pluginDir) {
        this.pluginName = requireNonNull(pluginName, "pluginName is null");
        this.pluginDir = requireNonNull(pluginDir, "pluginDir is null");
    }

    @Override
    public void loadPlugins(Loader loader, ClassLoaderFactory createClassLoader)
    {
        File pluginDirectory = new File(pluginDir);
        loader.load(pluginDirectory.getAbsolutePath(), () -> createClassLoader.create(pluginName, buildClassPath(pluginDirectory)));
    }

    private static List<URL> buildClassPath(File path)
    {
        return listFiles(path).stream()
                .map(DynamicPluginProvider::fileToUrl)
                .collect(toImmutableList());
    }

    private static List<File> listFiles(File path)
    {
        try {
            try (DirectoryStream<Path> directoryStream = newDirectoryStream(path.toPath())) {
                return stream(directoryStream)
                        .map(Path::toFile)
                        .sorted()
                        .collect(toImmutableList());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static URL fileToUrl(File file)
    {
        try {
            return file.toURI().toURL();
        }
        catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }
}
