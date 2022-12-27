/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.Plugin;
import io.trino.spi.classloader.ThreadContextClassLoader;

import javax.inject.Inject;

import java.net.URL;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.server.PluginManager.SPI_PACKAGES;
import static java.util.Arrays.asList;

public class DynamicFunctionPluginManager
{
    private static final Logger log = Logger.get(PluginManager.class);
    private final GlobalFunctionCatalog globalFunctionCatalog;

    @Inject
    public DynamicFunctionPluginManager(GlobalFunctionCatalog globalFunctionCatalog)
    {
        this.globalFunctionCatalog = globalFunctionCatalog;
    }

    public void loadFunctionPlugin(DynamicFunctionPluginProvider pluginProvider)
    {
        pluginProvider.loadPlugins(this::loadFunctionPlugin, DynamicFunctionPluginManager::createClassLoader);
    }

    private void loadFunctionPlugin(String plugin, Supplier<PluginClassLoader> createClassLoader)
    {
        log.info("-- Loading plugin %s --", plugin);

        PluginClassLoader pluginClassLoader = createClassLoader.get();

        log.debug("Classpath for plugin:");
        for (URL url : pluginClassLoader.getURLs()) {
            log.debug("    %s", url.getPath());
        }

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadFunctionPlugin(pluginClassLoader);
        }

        log.info("-- Finished loading plugin %s --", plugin);
    }

    private void loadFunctionPlugin(PluginClassLoader pluginClassLoader)
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);
        checkState(!plugins.isEmpty(), "No service providers of type %s in the classpath: %s", Plugin.class.getName(), asList(pluginClassLoader.getURLs()));

        for (Plugin plugin : plugins) {
            log.info("Installing %s", plugin.getClass().getName());
            installPluginInternal(plugin);
        }
    }

    private void installPluginInternal(Plugin plugin)
    {
        Set<Class<?>> functions = plugin.getFunctions();
        if (!functions.isEmpty()) {
            log.info("Registering functions from %s", plugin.getClass().getSimpleName());
            InternalFunctionBundle.InternalFunctionBundleBuilder builder = InternalFunctionBundle.builder();
            functions.forEach(builder::functions);
            globalFunctionCatalog.addFunctions(builder.build());
        }
    }

    public static PluginClassLoader createClassLoader(String pluginName, List<URL> urls)
    {
        ClassLoader parent = PluginManager.class.getClassLoader();
        return new PluginClassLoader(pluginName, urls, parent, SPI_PACKAGES);
    }
}
