package tech.dingxin.writers.type;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import tech.dingxin.writers.ArrowFieldWriter;

import java.nio.charset.StandardCharsets;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class StringWriter extends ArrowFieldWriter<String> {

    public StringWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(String row) {
        if (row == null) {
            ((VarCharVector) getValueVector()).setNull(getCount());
        } else {
            ((VarCharVector) getValueVector()).setSafe(getCount(), row.getBytes(StandardCharsets.UTF_8));
        }
    }
}
