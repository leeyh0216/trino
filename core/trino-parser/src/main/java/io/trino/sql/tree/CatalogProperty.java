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

public class CatalogProperty
        extends Node
{
    private final String name;
    private final Expression value;

    public CatalogProperty(String name, Expression value)
    {
        this(Optional.empty(), name, value);
    }

    public CatalogProperty(NodeLocation location, String name, Expression value)
    {
        this(Optional.of(location), name, value);
    }

    public CatalogProperty(Optional<NodeLocation> location, String name, Expression value)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.value = requireNonNull(value, "value is null");
    }

    public String getName()
    {
        return name;
    }

    public Expression getValue()
    {
        return value;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, value);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CatalogProperty other = (CatalogProperty) obj;
        return Objects.equals(name, other.name) &&
                Objects.equals(value, other.value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("name", name)
            .add("value", value)
            .toString();
    }
}
