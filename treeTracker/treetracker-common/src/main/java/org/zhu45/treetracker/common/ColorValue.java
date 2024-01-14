package org.zhu45.treetracker.common;

import lombok.Data;
import org.zhu45.treetracker.common.type.Type;

import java.util.Objects;

import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;

@Data
public class ColorValue
        extends AbstractRelationalValue
        implements Value
{
    public enum Color
    {
        Red,
        Green,
        Blue,
        Black,
        Grey,
        Orange,
        Navy,
        Purple,
        Turquoise,
        Yellow,
        Moss,
        Sky,
        White
    }

    private Color color;

    public ColorValue(Color color)
    {
        this.color = color;
        this.type = VARCHAR;
    }

    public ColorValue(Type type, Color color)
    {
        this.color = color;
        this.type = type;
    }

    @Override
    public String toString()
    {
        return this.color.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ColorValue o = (ColorValue) obj;
        return Objects.equals(o.color, this.color);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(color.toString());
    }
}
