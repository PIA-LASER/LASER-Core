package LASER.Utils;

import org.junit.*;
import static org.junit.Assert.*;

public class LaserUtilsTest {

    @Test
    public void testIntegerToIntegerIdMapping() {
        assert 1234 == LaserUtils.idToIndex(1234);
    }

    @Test
    public void testLongToIntegerIdMapping() {
        assertEquals(1557549057, LaserUtils.idToIndex(8000000000L));
    }
}
