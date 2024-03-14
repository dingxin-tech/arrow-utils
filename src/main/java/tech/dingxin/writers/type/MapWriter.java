package tech.dingxin.writers.type;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import tech.dingxin.writers.ArrowFieldWriter;
import tech.dingxin.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class MapWriter extends ArrowFieldWriter<Map> {
    private final ArrowFieldWriter<Object> keyWriter;
    private final ArrowFieldWriter<Object> valueWriter;
    protected StructVector structVector;

    public MapWriter(MapVector mapVector, ArrowFieldWriter keyWriter, ArrowFieldWriter valueWriter) {
        super(mapVector);
        this.structVector = (StructVector) mapVector.getDataVector();
        this.keyWriter = Preconditions.checkNotNull(keyWriter);
        this.valueWriter = Preconditions.checkNotNull(valueWriter);
    }

    @Override
    public void doWrite(Map row) {
        if (row != null) {
            ((MapVector) getValueVector()).startNewValue(getCount());
            List<Map.Entry> list = new ArrayList<>(row.entrySet());
            for (Map.Entry entry : list) {
                structVector.setIndexDefined(keyWriter.getCount());
                keyWriter.write(entry.getKey());
                valueWriter.write(entry.getValue());
            }
            ((MapVector) getValueVector()).endValue(getCount(), list.size());
        }
    }

    @Override
    public void finish() {
        super.finish();
        keyWriter.finish();
        valueWriter.finish();
    }

    @Override
    public void reset() {
        super.reset();
        keyWriter.reset();
        valueWriter.reset();
    }
}
