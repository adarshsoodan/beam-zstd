/*
 * Copyright Adarsh Soodan, 2020
 * Licensed under http://www.apache.org/licenses/LICENSE-2.0
 */
package in.neolog.beam.coder;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.vendor.grpc.v1p26p0.org.bouncycastle.util.Arrays;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.Fields;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;

@RunWith(JUnitQuickcheck.class)
public class TestZstdCoder {

    @Property(shrink = false)
    public void testWithSerializableCoder(@From(Fields.class) SampleBean bean) throws CoderException, IOException {
        SerializableCoder<SampleBean> serializableCoder = SerializableCoder.of(SampleBean.class);
        ZstdCoder<SampleBean> zstdCoder = ZstdCoder.of(serializableCoder);

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        zstdCoder.encode(bean, outStream);
        byte[] encoded = outStream.toByteArray();

        ByteArrayInputStream inStream = new ByteArrayInputStream(encoded);

        SampleBean decoded = zstdCoder.decode(inStream);

        assertEquals(bean, decoded);
    }

    @Test
    public void testSmallerSizeWithSerializableCoder() throws CoderException, IOException {
        SerializableCoder<SampleBean> serializableCoder = SerializableCoder.of(SampleBean.class);
        ZstdCoder<SampleBean> zstdCoder = ZstdCoder.of(serializableCoder);

        SampleBean bean = new SampleBean();
        bean.setStr("qwerty");
        bean.setX(11);
        byte[] b = new byte[1024];
        Arrays.fill(b, (byte) 1);
        bean.setBytes(b);

        ByteArrayOutputStream outzstdStream = new ByteArrayOutputStream();
        zstdCoder.encode(bean, outzstdStream);
        byte[] compressed = outzstdStream.toByteArray();

        ByteArrayOutputStream outuncompStream = new ByteArrayOutputStream();
        serializableCoder.encode(bean, outuncompStream);
        byte[] uncompressed = outuncompStream.toByteArray();

        MatcherAssert.assertThat("Zstd compressed array size = " + compressed.length + ", SerialzedCoder array size = "
                + uncompressed.length, compressed.length < uncompressed.length);
    }

    @Test
    public void testLargerSizeWithSerializableCoder() throws CoderException, IOException {
        SerializableCoder<SampleBean> serializableCoder = SerializableCoder.of(SampleBean.class);
        ZstdCoder<SampleBean> zstdCoder = ZstdCoder.of(serializableCoder);

        SampleBean bean = new SampleBean();
        bean.setStr("");
        bean.setX(11);
        bean.setBytes(new byte[] {});

        ByteArrayOutputStream outzstdStream = new ByteArrayOutputStream();
        zstdCoder.encode(bean, outzstdStream);
        byte[] compressed = outzstdStream.toByteArray();

        ByteArrayOutputStream outuncompStream = new ByteArrayOutputStream();
        serializableCoder.encode(bean, outuncompStream);
        byte[] uncompressed = outuncompStream.toByteArray();

        MatcherAssert.assertThat("Zstd compressed array size = " + compressed.length + ", SerialzedCoder array size = "
                + uncompressed.length, compressed.length > uncompressed.length);
    }

}
