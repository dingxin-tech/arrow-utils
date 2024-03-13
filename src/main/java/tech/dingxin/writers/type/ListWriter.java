package tech.dingxin.writers.type;

import java.util.List;

import org.apache.arrow.vector.complex.ListVector;
import tech.dingxin.common.ArrowFieldWriter;
import tech.dingxin.common.Preconditions;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ListWriter extends ArrowFieldWriter<List> {
    private final ArrowFieldWriter<Object> elementWriter;

    public ListWriter(ListVector mapVector, ArrowFieldWriter valueWriter) {
        super(mapVector);
        this.elementWriter = Preconditions.checkNotNull(valueWriter);
    }

    @Override
    public void doWrite(List row, int ordinal) {
        if (row != null) {
            ((ListVector)getValueVector()).startNewValue(getCount());
            for (int i = 0; i < row.size(); i++) {
                elementWriter.write(row.get(i), i);
            }
            ((ListVector)getValueVector()).endValue(getCount(), row.size());
        }
    }

    @Override
    public void finish() {
        super.finish();
        elementWriter.finish();
    }

    @Override
    public void reset() {
        super.reset();
        elementWriter.reset();
    }
}
