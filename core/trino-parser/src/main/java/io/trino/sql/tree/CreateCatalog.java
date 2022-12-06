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

public class CreateCatalog
        extends Statement
{
    private final QualifiedName catalogName;

    private final String pluginName;
    private final boolean notExists;
    private final List<CatalogProperty> properties;

    public CreateCatalog(QualifiedName catalogName, String pluginName, boolean notExists, List<CatalogProperty> properties)
    {
        this(Optional.empty(), catalogName, pluginName, notExists, properties);
    }

    public CreateCatalog(NodeLocation location, QualifiedName catalogName, String pluginName, boolean notExists, List<CatalogProperty> properties)
    {
        this(Optional.of(location), catalogName, pluginName, notExists, properties);
    }

    private CreateCatalog(Optional<NodeLocation> location, QualifiedName catalogName, String pluginName, boolean notExists, List<CatalogProperty> properties)
    {
        super(location);
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.pluginName = requireNonNull(pluginName, "pluginName is null");
        this.notExists = notExists;
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
    }

    public QualifiedName getCatalogName()
    {
        return catalogName;
    }

    public String getPluginName()
    {
        return pluginName;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public List<CatalogProperty> getProperties()
    {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateCatalog(this, context);
    }

    @Override
    public List<CatalogProperty> getChildren()
    {
        return properties;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, notExists, properties);
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
        CreateCatalog o = (CreateCatalog) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(notExists, o.notExists) &&
                Objects.equals(properties, o.properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("pluginName", pluginName)
                .add("notExists", notExists)
                .add("properties", properties)
                .toString();
    }
}
