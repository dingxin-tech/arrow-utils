package tech.dingxin.writers.type;

import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import tech.dingxin.writers.ArrowFieldWriter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class DoubleWriter extends ArrowFieldWriter<Double> {

    public DoubleWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(Double row) {
        if (row == null) {
            ((Float8Vector) getValueVector()).setNull(getCount());
        } else {
            ((Float8Vector) getValueVector()).setSafe(getCount(), row);
        }
    }
}
