package tech.dingxin.writers.type;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import tech.dingxin.common.ArrowFieldWriter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class IntegerWriter extends ArrowFieldWriter<Integer> {

    public IntegerWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(Integer row, int ordinal) {
        if (row == null) {
            ((IntVector)getValueVector()).setNull(getCount());
        } else {
            ((IntVector)getValueVector()).setSafe(getCount(), row);
        }
    }
}
