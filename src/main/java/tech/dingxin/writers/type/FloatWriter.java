package tech.dingxin.writers.type;

import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.ValueVector;
import tech.dingxin.common.ArrowFieldWriter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class FloatWriter extends ArrowFieldWriter<Float> {

    public FloatWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(Float row, int ordinal) {
        if (row == null) {
            ((Float4Vector)getValueVector()).setNull(getCount());
        } else {
            ((Float4Vector)getValueVector()).setSafe(getCount(), row);
        }
    }
}
