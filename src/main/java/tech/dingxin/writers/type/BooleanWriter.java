package tech.dingxin.writers.type;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;
import tech.dingxin.common.ArrowFieldWriter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class BooleanWriter extends ArrowFieldWriter<Boolean> {

    public BooleanWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(Boolean row, int ordinal) {
        if (row == null) {
            ((BitVector)getValueVector()).setNull(getCount());
        } else {
            ((BitVector)getValueVector()).setSafe(getCount(), row ? 1 : 0);
        }
    }
}
