package tech.dingxin;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.dingxin.utils.POJO;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
class PojoToArrowConverterTest {
    @Test
    void testE2E() {
        PojoToArrowConverter pojoToArrowConverter = new PojoToArrowConverter(POJO.class, null);
        pojoToArrowConverter.newInstance();
        POJO pojo = POJO.getSampleInstance();

        pojoToArrowConverter.write(pojo);
        VectorSchemaRoot arrowBatch1 = pojoToArrowConverter.getArrowBatch();
        Assertions.assertEquals(1, arrowBatch1.getRowCount());

        pojoToArrowConverter.reset();
        pojoToArrowConverter.write(pojo);
        pojoToArrowConverter.write(pojo);
        VectorSchemaRoot arrowBatch2 = pojoToArrowConverter.getArrowBatch();
        Assertions.assertEquals(2, arrowBatch2.getRowCount());

        pojoToArrowConverter.reset();
        pojoToArrowConverter.write(pojo);
        pojoToArrowConverter.write(pojo);
        pojoToArrowConverter.write(pojo);
        VectorSchemaRoot arrowBatch3 = pojoToArrowConverter.getArrowBatch();
        Assertions.assertEquals(3, arrowBatch3.getRowCount());
    }
}
