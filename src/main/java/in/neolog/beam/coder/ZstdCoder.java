/*
 * Copyright Adarsh Soodan, 2020
 * Licensed under http://www.apache.org/licenses/LICENSE-2.0
 */
package in.neolog.beam.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.CoderUtils;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;

public class ZstdCoder<T> extends StructuredCoder<T> {
    private static final long serialVersionUID = 1L;

    /**
     * Can be overriden in {@link ZstdCoder#of(Coder, int)}
     */
    public static final int MAX_DECOMPRESSED_SIZE     = 8 * 1024 * 1024;
    public static final int DEFAULT_COMPRESSION_LEVEL = 3;

    private final Coder<T> innerCoder;
    private final int      maxDecompressedSize;
    private final byte[]   dictionary;

    private transient volatile ZstdDictCompress   nativeCompressDict   = null;
    private transient volatile ZstdDictDecompress nativeDecompressDict = null;

    /**
     * Wraps the given coder into a {@link ZstdCoder}. Assumes maxDecompressedSize =
     * {@link ZstdCoder#MAX_DECOMPRESSED_SIZE}
     * 
     * @param innerCoder The output of innerCoder is compressed. The uncompressed
     *                   data becomes input if innercoder
     */
    public static <T> ZstdCoder<T> of(Coder<T> innerCoder) {
        return new ZstdCoder<>(innerCoder);
    }

    /**
     * Wraps the given coder into a {@link ZstdCoder}.
     * 
     * @param innerCoder          The output of innerCoder is compressed. The
     *                            uncompressed data becomes input if innercoder
     * @param dictionary          Optional dictionary used to compress/decompress.
     *                            It improves compression ratios.
     * @param maxDecompressedSize The maximum size of decompressed byte array. <br>
     *                            Note: For all data compressed using this coder,
     *                            maximum size is detected from compressed data.
     *                            Only if such detection fails is this value used.
     */
    public static <T> ZstdCoder<T> of(Coder<T> innerCoder, byte[] dictionary, int maxDecompressedSize) {
        return new ZstdCoder<>(innerCoder, dictionary, maxDecompressedSize);
    }

    private ZstdCoder(Coder<T> innerCoder) {
        this(innerCoder, null, MAX_DECOMPRESSED_SIZE);
    }

    private ZstdCoder(Coder<T> innerCoder, byte[] dictionary, int maxDecompressedSize) {
        this.innerCoder = innerCoder;
        this.maxDecompressedSize = maxDecompressedSize;
        this.dictionary = dictionary;
    }

    @Override
    public void encode(T value, OutputStream outStream) throws CoderException, IOException {
        if (dictionary != null && nativeCompressDict == null) {
            nativeCompressDict = new ZstdDictCompress(dictionary, DEFAULT_COMPRESSION_LEVEL);
        }

        byte[] innerBytes = CoderUtils.encodeToByteArray(innerCoder, value);

        byte[] compressed;
        if (nativeCompressDict != null) {
            compressed = Zstd.compress(innerBytes, nativeCompressDict);
        } else {
            compressed = Zstd.compress(innerBytes);
        }

        ByteArrayCoder.of()
                .encode(compressed, outStream);
    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
        if (dictionary != null && nativeDecompressDict == null) {
            nativeDecompressDict = new ZstdDictDecompress(dictionary);
        }

        byte[] compressed = ByteArrayCoder.of()
                .decode(inStream);

        long decompressedSize = Zstd.decompressedSize(compressed);
        if (decompressedSize == 0) {
            decompressedSize = maxDecompressedSize;
        }

        byte[] uncompressed;
        if (nativeCompressDict != null) {
            uncompressed = Zstd.decompress(compressed, nativeDecompressDict, (int) decompressedSize);
        } else {
            uncompressed = Zstd.decompress(compressed, (int) decompressedSize);
        }
        T decoded = CoderUtils.decodeFromByteArray(innerCoder, uncompressed);
        return decoded;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.singletonList(innerCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        innerCoder.verifyDeterministic();
    }

    /**
     * See {@link ZstdCoder#of(Coder, byte[], int)
     */
    public static <T> ZstdCoder<T> of(Coder<T> innerCoder, byte[] dict) {
        return of(innerCoder, dict, MAX_DECOMPRESSED_SIZE);
    }

    /**
     * Utility method for training Zstd dictionaries.
     * 
     * @param innerCoder      The inner coder that will be used in
     *                        {@link ZstdCoder#of(Coder, byte[])}
     * @param trainingSamples Enough training samples to build the dictionary. .
     * @param dictSize        Desired size of dictionary. The returned dictionary
     *                        can be smaller than this.
     * @return
     * @throws CoderException
     * @throws {@link         RuntimeException} Zstd throws an error if enough
     *                        samples are not given or if dictionary size is too
     *                        small.
     */
    public static <T> byte[] trainDictionary(Coder<T> innerCoder, List<T> trainingSamples, int dictSize)
            throws CoderException {
        byte[] dict = new byte[dictSize];

        byte[][] trainOn = new byte[trainingSamples.size()][];
        for (int i = 0; i < trainOn.length; i++) {
            trainOn[i] = CoderUtils.encodeToByteArray(innerCoder, trainingSamples.get(i));
        }

        long sizeOrErr = Zstd.trainFromBuffer(trainOn, dict);
        boolean isErr = Zstd.isError(sizeOrErr);
        if (!isErr) {
            return Arrays.copyOf(dict, (int) sizeOrErr);
        } else {
            String errorName = Zstd.getErrorName(sizeOrErr);
            throw new RuntimeException(
                    String.format("Zstd Dictionary training error code = (%s, %s)", sizeOrErr, errorName));
        }

    }
}
