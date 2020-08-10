/*
 * Copyright Adarsh Soodan, 2020
 * Licensed under http://www.apache.org/licenses/LICENSE-2.0
 */
package in.neolog.beam.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

import com.github.luben.zstd.Zstd;

public class ZstdCoder<T> extends StructuredCoder<T> {
    private static final long serialVersionUID      = 1L;

    private static final int  MAX_DECOMPRESSED_SIZE = 8 * 1024 * 1024;
    private final Coder<T>    innerCoder;

    /** Wraps the given coder into a {@link ZstdCoder}. */
    public static <T> ZstdCoder<T> of(Coder<T> innerCoder) {
        return new ZstdCoder<>(innerCoder);
    }

    private ZstdCoder(Coder<T> innerCoder) {
        this.innerCoder = innerCoder;
    }

    @Override
    public void encode(T value, OutputStream outStream) throws CoderException, IOException {
        byte[] innerBytes = CoderUtils.encodeToByteArray(innerCoder, value);
        byte[] compressed = Zstd.compress(innerBytes);
        ByteArrayCoder.of()
                .encode(compressed, outStream);
    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
        byte[] compressed = ByteArrayCoder.of()
                .decode(inStream);
        long decompressedSize = Zstd.decompressedSize(compressed);
        if (decompressedSize == 0) {
            decompressedSize = MAX_DECOMPRESSED_SIZE;
        }
        byte[] uncompressed = Zstd.decompress(compressed, (int) decompressedSize);
        T decoded = CoderUtils.decodeFromByteArray(innerCoder, uncompressed);
        return decoded;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return ImmutableList.of(innerCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        innerCoder.verifyDeterministic();
    }

}
