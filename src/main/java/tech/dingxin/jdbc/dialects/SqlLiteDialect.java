package tech.dingxin.jdbc.dialects;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import tech.dingxin.jdbc.JdbcDialect;

import java.sql.ResultSetMetaData;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SqlLiteDialect implements JdbcDialect {
    public static final JdbcDialect INSTANCE = new SqlLiteDialect();

    @Override
    public ArrowType getArrowType(int sqlType, String typeName, int size, ResultSetMetaData md) {
        if ("BOOLEAN".equals(typeName)) {
            return new ArrowType.Bool();
        }
        if ("TINYINT".equals(typeName)) {
            return new ArrowType.Int(8, true);
        }
        if ("SMALLINT".equals(typeName) || "INT2".equals(typeName)) {
            return new ArrowType.Int(16, true);
        }
        if ("BIGINT".equals(typeName) || "INT8".equals(typeName) || "UNSIGNED BIG INT".equals(typeName)) {
            return new ArrowType.Int(64, true);
        }
        if ("INT".equals(typeName) || "INTEGER".equals(typeName) || "MEDIUMINT".equals(typeName)) {
            return new ArrowType.Int(32, true);
        }
        if ("DATE".equals(typeName)) {
            return Types.MinorType.DATEDAY.getType();
        }
        if ("DATETIME".equals(typeName)) {
            return Types.MinorType.TIMESTAMPMILLI.getType();
        }
        if ("TIMESTAMP".equals(typeName)) {
            return Types.MinorType.TIMESTAMPMILLI.getType();
        }
        if ("DECIMAL".equals(typeName)) {
            return Types.MinorType.DECIMAL.getType();
        }
        if ("DOUBLE".equals(typeName) || "DOUBLE PRECISION".equals(typeName)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        if ("FLOAT".equals(typeName)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }
        if ("CHARACTER".equals(typeName) || "NCHAR".equals(typeName) || "NATIVE CHARACTER".equals(typeName) ||
                "CHAR".equals(typeName)) {
            return new ArrowType.Utf8();
        }
        if ("VARCHAR".equals(typeName) || "VARYING CHARACTER".equals(typeName) ||
                "NVARCHAR".equals(typeName) || "TEXT".equals(typeName)) {
            return new ArrowType.Utf8();
        }

        if ("BINARY".equals(typeName) || "BLOB".equals(typeName)) {
            return Types.MinorType.VARBINARY.getType();
        }
        throw new UnsupportedOperationException("Unsupported type: " + typeName);
    }
}
