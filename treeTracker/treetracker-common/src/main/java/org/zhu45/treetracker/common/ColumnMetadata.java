package org.zhu45.treetracker.common;

import org.zhu45.treetracker.common.type.Type;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ColumnMetadata
{
    private final String name;
    private final Type type;
    private final boolean nullable;
    private final String comment;
    private final String extraInfo;
    private final boolean hidden;
    private final Map<String, Object> properties;

    public ColumnMetadata(String name, Type type)
    {
        this(name, type, true, null, null, false, emptyMap());
    }

    public ColumnMetadata(String name, Type type, String comment, boolean hidden)
    {
        this(name, type, true, comment, null, hidden, emptyMap());
    }

    public ColumnMetadata(String name, Type type, String comment, String extraInfo, boolean hidden)
    {
        this(name, type, true, comment, extraInfo, hidden, emptyMap());
    }

    public ColumnMetadata(String name, Type type, String comment, String extraInfo, boolean hidden, Map<String, Object> properties)
    {
        this(name, type, true, comment, extraInfo, hidden, properties);
    }

    public ColumnMetadata(String name, Type type, boolean nullable, String comment, String extraInfo, boolean hidden, Map<String, Object> properties)
    {
        SchemaUtil.checkNotEmpty(name, "name");
        requireNonNull(type, "type is null");
        requireNonNull(properties, "properties is null");

        this.name = name.toLowerCase(ENGLISH);
        this.type = type;
        this.comment = comment;
        this.extraInfo = extraInfo;
        this.hidden = hidden;
        this.properties = properties.isEmpty() ? emptyMap() : unmodifiableMap(new LinkedHashMap<>(properties));
        this.nullable = nullable;
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public String getComment()
    {
        return comment;
    }

    public String getExtraInfo()
    {
        return extraInfo;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    public Map<String, Object> getProperties()
    {
        return properties;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ColumnMetadata{");
        sb.append("name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", ").append(nullable ? "nullable" : "nonnull");
        if (comment != null) {
            sb.append(", comment='").append(comment).append('\'');
        }
        if (extraInfo != null) {
            sb.append(", extraInfo='").append(extraInfo).append('\'');
        }
        if (hidden) {
            sb.append(", hidden");
        }
        if (!properties.isEmpty()) {
            sb.append(", properties=").append(properties);
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, nullable, comment, extraInfo, hidden);
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
        ColumnMetadata other = (ColumnMetadata) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.nullable, other.nullable) &&
                Objects.equals(this.comment, other.comment) &&
                Objects.equals(this.extraInfo, other.extraInfo) &&
                Objects.equals(this.hidden, other.hidden);
    }
}
