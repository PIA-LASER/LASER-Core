package LASER.mapreduce.preparation;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.VarIntWritable;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class ToUserVectorMapperTest {

    @Test
    public void testPartialUserPreferenceVector() throws IOException, InterruptedException{
        ToUserVectorMapper mapper = new ToUserVectorMapper();

        Text input = new Text("1,123456,0.9");

        Mapper.Context context = mock(Mapper.Context.class);

        mapper.map(null, input, context);

        verify(context, times(1)).write(new VarIntWritable(1), new EntityPrefWritable(123456, 0.9f));
    }
}
