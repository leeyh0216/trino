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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LoadFunction
        extends Statement
{
    private final QualifiedName pluginName;
    private final String pluginDir;

    public LoadFunction(QualifiedName pluginName, String pluginDir)
    {
        this(Optional.empty(), pluginName, pluginDir);
    }

    public LoadFunction(NodeLocation location, QualifiedName pluginName, String pluginDir)
    {
        this(Optional.of(location), pluginName, pluginDir);
    }

    public LoadFunction(Optional<NodeLocation> location, QualifiedName pluginName, String pluginDir)
    {
        super(location);
        this.pluginName = requireNonNull(pluginName, "pluginName is null");
        this.pluginDir = requireNonNull(pluginDir, "pluginDir is null");
    }

    public QualifiedName getPluginName()
    {
        return pluginName;
    }

    public String getPluginDir()
    {
        return pluginDir;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(pluginName, pluginDir);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        LoadFunction o = (LoadFunction) obj;
        return Objects.equals(pluginName, o.pluginName) &&
                Objects.equals(pluginDir, o.pluginDir);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("pluginName", pluginName)
                .add("pluginDir", pluginDir)
                .toString();
    }
}
