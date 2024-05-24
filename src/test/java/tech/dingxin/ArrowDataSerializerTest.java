package tech.dingxin;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
class ArrowDataSerializerTest {

    private static Schema testSchema;

    @BeforeAll
    public static void init() {
        List<Field> fields = new ArrayList<>();
        fields.add(Field.nullable("name", ArrowType.Utf8.INSTANCE));
        fields.add(Field.nullable("age", new ArrowType.Int(32, true)));
        testSchema = new Schema(fields);
    }

    @Test
    void testE2E() {
        ArrowDataSerializer arrowDataSerializer = new ArrowDataSerializer(testSchema, new RootAllocator());

        ArrowRowData rowData = new ArrowRowData(testSchema);
        rowData.set(0, "Jack");
        rowData.set(1, 18);

        for (int i = 0; i < 10; i++) {
            arrowDataSerializer.add(rowData);
        }
        Assertions.assertEquals(10, arrowDataSerializer.getVectorSchemaRoot().getRowCount());

        rowData.set(0, "Tom");
        rowData.set(1, 20);
        for (int i = 0; i < 5; i++) {
            arrowDataSerializer.add(rowData);
        }
        Assertions.assertEquals(15, arrowDataSerializer.getVectorSchemaRoot().getRowCount());


        arrowDataSerializer.reset();
        Assertions.assertEquals(0, arrowDataSerializer.getVectorSchemaRoot().getRowCount());
    }
}
