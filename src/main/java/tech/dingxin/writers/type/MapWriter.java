package tech.dingxin.writers.type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import tech.dingxin.common.ArrowFieldWriter;
import tech.dingxin.common.Preconditions;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class MapWriter extends ArrowFieldWriter<Map> {
    protected StructVector structVector;
    private final ArrowFieldWriter<Object> keyWriter;
    private final ArrowFieldWriter<Object> valueWriter;

    public MapWriter(MapVector mapVector, ArrowFieldWriter keyWriter, ArrowFieldWriter valueWriter) {
        super(mapVector);
        this.structVector = (StructVector)mapVector.getDataVector();
        this.keyWriter = Preconditions.checkNotNull(keyWriter);
        this.valueWriter = Preconditions.checkNotNull(valueWriter);
    }

    @Override
    public void doWrite(Map row, int ordinal) {
        if (row != null) {
            ((MapVector)getValueVector()).startNewValue(getCount());
            List<Map.Entry> list = new ArrayList<>(row.entrySet());
            for (int i = 0; i < list.size(); i++) {
                structVector.setIndexDefined(keyWriter.getCount());
                keyWriter.write(list.get(i).getKey(), i);
                valueWriter.write(list.get(i).getValue(), i);
            }
            ((MapVector)getValueVector()).endValue(getCount(), list.size());
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
