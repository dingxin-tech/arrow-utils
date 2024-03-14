package tech.dingxin.writers.type;

import org.apache.arrow.vector.complex.ListVector;
import tech.dingxin.writers.ArrowFieldWriter;
import tech.dingxin.utils.Preconditions;

import java.util.List;

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
    public void doWrite(List row) {
        if (row != null) {
            ((ListVector) getValueVector()).startNewValue(getCount());
            for (Object o : row) {
                elementWriter.write(o);
            }
            ((ListVector) getValueVector()).endValue(getCount(), row.size());
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
