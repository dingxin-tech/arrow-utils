package tech.dingxin;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import tech.dingxin.utils.Preconditions;
import tech.dingxin.writers.ArrowWriter;
import tech.dingxin.writers.ArrowWriterFactory;

import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ArrowDataSerializer implements AutoCloseable {
    private final VectorSchemaRoot vectorSchemaRoot;
    private final ArrowWriter<Object> arrowWriter;
    private int rowCount;

    public ArrowDataSerializer(Schema schema, BufferAllocator allocator) {
        Preconditions.checkNotNull(schema);
        Preconditions.checkNotNull(allocator);
        rowCount = 0;
        this.vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        this.arrowWriter = ArrowWriterFactory.createArrowWriter(vectorSchemaRoot);
    }

    public void addAll(List<ArrowRowData> rowDataList) {
        for (ArrowRowData rowData : rowDataList) {
            add(rowData);
        }
    }

    public void add(ArrowRowData rowData) {
        rowCount++;
        for (int i = 0; i < rowData.getLength(); i++) {
            arrowWriter.write(rowData.get(i), rowData.getName(i));
        }
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        arrowWriter.finish();
        vectorSchemaRoot.setRowCount(rowCount);
        return vectorSchemaRoot;
    }

    public void reset() {
        rowCount = 0;
        arrowWriter.reset();
    }

    @Override
    public void close() throws Exception {
        vectorSchemaRoot.close();
    }
}
