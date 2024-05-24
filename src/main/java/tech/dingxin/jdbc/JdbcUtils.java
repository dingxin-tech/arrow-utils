package tech.dingxin.jdbc;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class JdbcUtils {

    public static Schema toArrowSchema(ResultSetMetaData metaData, JdbcDialect jdbcDialect) throws Exception {
        int columnCount = metaData.getColumnCount();
        List<Field> fieldList = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            ArrowType arrowType = jdbcDialect.getArrowType(metaData.getColumnType(i), metaData.getColumnTypeName(i), metaData.getPrecision(i), metaData);
            if (arrowType == null) {
                throw new IllegalArgumentException("Unsupported type: " + metaData.getColumnTypeName(i));
            }
            if (arrowType.isComplex()) {
                Field complexField = jdbcDialect.getComplexArrowField(metaData.getColumnType(i), metaData.getColumnTypeName(i), metaData.getPrecision(i), metaData);
                fieldList.add(complexField);
            } else {
                fieldList.add(new Field(columnName, FieldType.nullable(arrowType), null));
            }
        }
        return new Schema(fieldList);
    }
}
