import org.apache.arrow.vector.VectorSchemaRoot;
import tech.dingxin.POJO2ArrowConverter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class POJO2ArrowConverterTest {
    public static void main(String[] args) {
        POJO2ArrowConverter pojo2ArrowConverter = new POJO2ArrowConverter(POJO.class, null);
        pojo2ArrowConverter.newInstance();
        POJO pojo = POJO.getSampleInstance();

        pojo2ArrowConverter.write(pojo);
        VectorSchemaRoot arrowBatch1 = pojo2ArrowConverter.getArrowBatch();
        System.out.println(arrowBatch1.contentToTSVString());

        pojo2ArrowConverter.reset();
        pojo2ArrowConverter.write(pojo);
        pojo2ArrowConverter.write(pojo);
        VectorSchemaRoot arrowBatch2 = pojo2ArrowConverter.getArrowBatch();
        System.out.println(arrowBatch2.contentToTSVString());
    }
}
