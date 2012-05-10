package LASER.Utils;

import com.google.common.primitives.Longs;

public class LaserUtils {

    public static int idToIndex(long id) {
        return 0x7FFFFFFF & Longs.hashCode(id);
    }

}
