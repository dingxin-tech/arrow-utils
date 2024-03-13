package tech.dingxin.writers.type;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import tech.dingxin.common.ArrowFieldWriter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class LongWriter extends ArrowFieldWriter<Long> {

    public LongWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(Long row, int ordinal) {
        if (row == null) {
            ((BigIntVector)getValueVector()).setNull(getCount());
        } else {
            ((BigIntVector)getValueVector()).setSafe(getCount(), row);
        }
    }
}
