/*
 * Copyright Adarsh Soodan, 2020
 * Licensed under http://www.apache.org/licenses/LICENSE-2.0
 */
package in.neolog.beam.coder;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.commons.io.IOUtils;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.github.luben.zstd.Zstd;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.Fields;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;

@RunWith(JUnitQuickcheck.class)
public class TestZstdCoder {

    @Property(shrink = false)
    public void testWithoutDictionary(@From(Fields.class) SampleBean bean) throws CoderException, IOException {
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
    public void testCompressionReducesSize() throws CoderException, IOException {
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
    public void testCompressionIncreasesSizeForSmallObject() throws CoderException, IOException {
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

    @Test
    public void testWithDictionary() throws CoderException, IOException {
        try (InputStream wapStream = TestZstdCoder.class.getResourceAsStream("/war and peace.txt")) {

            String input = new String(IOUtils.toByteArray(wapStream), StandardCharsets.UTF_8);
            String[] lines = input.split("((?<=(\\r\\n))|(?<=(\\n)))");

            int chunkSize = 1024;
            byte[][] trainOn;
            {
                trainOn = Arrays.stream(lines)
                        .collect(() -> new ArrayList<ByteArrayOutputStream>(), (arrs, line) -> {
                            if (arrs.isEmpty()) {
                                arrs.add(new ByteArrayOutputStream());
                            }
                            byte[] lineBytes = line.getBytes(StandardCharsets.UTF_8);
                            ByteArrayOutputStream last = arrs.get(arrs.size() - 1);
                            if (last.size() + lineBytes.length > chunkSize) {
                                arrs.add(new ByteArrayOutputStream());
                                last = arrs.get(arrs.size() - 1);
                            }
                            try {
                                last.write(lineBytes);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }, (arrs1, arrs2) -> {
                            arrs1.addAll(arrs2);
                        })
                        .stream()
                        .map(baos -> baos.toByteArray())
                        .collect(Collectors.toList())
                        .toArray(new byte[][] {});
            }

            List<SampleBean> trainingSamples = new ArrayList<>();
            for (int i = 0; i < trainOn.length / 10; i++) {
                SampleBean b = new SampleBean();
                b.setX(i);
                b.setStr(new String(trainOn[i], StandardCharsets.UTF_8));
                b.setBytes(new byte[0]);
                trainingSamples.add(b);
            }
            SerializableCoder<SampleBean> innerCoder = SerializableCoder.of(SampleBean.class);

            byte[] dict = ZstdCoder.trainDictionary(innerCoder, trainingSamples, 10 * 1024);
            ZstdCoder<SampleBean> zstdCoder = ZstdCoder.of(innerCoder, dict);

            List<SampleBean> expected = new ArrayList<>();
            List<SampleBean> actual = new ArrayList<>();
            for (int i = 0; i < lines.length; i++) {
                SampleBean b = new SampleBean();
                b.setX(i);
                b.setStr(lines[i]);
                b.setBytes(new byte[0]);
                expected.add(b);

                ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                zstdCoder.encode(b, outStream);
                byte[] encoded = outStream.toByteArray();

                ByteArrayInputStream inStream = new ByteArrayInputStream(encoded);
                SampleBean decoded = zstdCoder.decode(inStream);
                actual.add(decoded);
            }
            assertEquals(expected, actual);
        }
    }
}
