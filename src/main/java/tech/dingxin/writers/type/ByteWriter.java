package tech.dingxin.writers.type;

import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import tech.dingxin.writers.ArrowFieldWriter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ByteWriter extends ArrowFieldWriter<Byte> {

    public ByteWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(Byte row) {
        if (row == null) {
            ((TinyIntVector) getValueVector()).setNull(getCount());
        } else {
            ((TinyIntVector) getValueVector()).setSafe(getCount(), row);
        }
    }
}
