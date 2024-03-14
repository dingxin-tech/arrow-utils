package tech.dingxin.writers.type;

import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.ValueVector;
import tech.dingxin.writers.ArrowFieldWriter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ShortWriter extends ArrowFieldWriter<Short> {

    public ShortWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(Short row) {
        if (row == null) {
            ((SmallIntVector) getValueVector()).setNull(getCount());
        } else {
            ((SmallIntVector) getValueVector()).setSafe(getCount(), row);
        }
    }
}
