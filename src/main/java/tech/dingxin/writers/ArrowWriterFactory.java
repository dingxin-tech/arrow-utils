package tech.dingxin.writers;

import java.util.List;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import tech.dingxin.common.ArrowFieldWriter;
import tech.dingxin.writers.type.BooleanWriter;
import tech.dingxin.writers.type.ByteWriter;
import tech.dingxin.writers.type.DoubleWriter;
import tech.dingxin.writers.type.FloatWriter;
import tech.dingxin.writers.type.IntegerWriter;
import tech.dingxin.writers.type.ListWriter;
import tech.dingxin.writers.type.LongWriter;
import tech.dingxin.writers.type.MapWriter;
import tech.dingxin.writers.type.ShortWriter;
import tech.dingxin.writers.type.StringWriter;
import tech.dingxin.writers.type.StructWriter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ArrowWriterFactory {
    private ArrowWriterFactory() {
    }
    public static ArrowFieldWriter createFieldWriter(ValueVector vector) {
        if (vector instanceof TinyIntVector) {
            return new ByteWriter(vector);
        } else if (vector instanceof SmallIntVector) {
            return new ShortWriter(vector);
        } else if (vector instanceof BigIntVector) {
            return new LongWriter(vector);
        } else if (vector instanceof BitVector) {
            return new BooleanWriter(vector);
        } else if (vector instanceof Float4Vector) {
            return new FloatWriter(vector);
        } else if (vector instanceof Float8Vector) {
            return new DoubleWriter(vector);
        } else if (vector instanceof VarCharVector) {
            return new StringWriter(vector);
        } else if (vector instanceof IntVector) {
            return new IntegerWriter(vector);
        } else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector)vector;
            StructVector structVector = (StructVector)mapVector.getDataVector();
            return new MapWriter(mapVector, createFieldWriter(structVector.getChild(MapVector.KEY_NAME)),
                                 createFieldWriter(structVector.getChild(MapVector.VALUE_NAME)));
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector)vector;
            FieldVector elementVector = listVector.getDataVector();
            return new ListWriter(listVector, createFieldWriter(elementVector));
        } else if (vector instanceof StructVector) {
            StructVector structVector = (StructVector)vector;
            List<FieldVector> childrenVectors = structVector.getChildrenFromFields();
            ArrowFieldWriter[] fieldsWriters = new ArrowFieldWriter[childrenVectors.size()];
            for (int i = 0; i < fieldsWriters.length; i++) {
                fieldsWriters[i] = createFieldWriter(((StructVector)vector).getVectorById(i));
            }
            return new StructWriter(structVector, fieldsWriters);
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported type %s.", vector.getName()));
        }
    }

    public static ArrowWriter<Object> createArrowWriter(VectorSchemaRoot root) {
        ArrowFieldWriter<Object>[] fieldWriters = new ArrowFieldWriter[root.getFieldVectors().size()];
        List<FieldVector> vectors = root.getFieldVectors();
        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            vector.allocateNew();
            fieldWriters[i] = createFieldWriter(vector);
        }
        return new ArrowWriter<>(root, fieldWriters);
    }
}
