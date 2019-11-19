package com.fanwill.codec;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: Will Fan
 * Created: 2019/10/10 12:36
 * Description:
 */
public final class Decoder {
    private static String decodeString(final ByteBuf in) {

        if (in.readableBytes() < Constants.INT_LEN) {
            return null;
        }

        int strLen = in.readUnsignedShort();
        if (strLen == 0 || in.readableBytes() < strLen) {
            return null;
        }
        byte[] strRaw = new byte[strLen];
        in.readBytes(strRaw);

        try {
            return new String(strRaw, Constants.CHAR_SET_NAME);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static int decodeInt2(final byte[] bs) {

        if (bs.length < Constants.INT_LEN) {
            throw new IllegalArgumentException("the len of byte array must be greater than 2.. ");
        }

        return bs[0] << 8 & 0xFF00 | bs[1] & 0xFF;
    }

    public static Map<String, String> decodeMap(final ByteBuf in) {

        final Map<String, String> ret = new HashMap<>();

        decodeMap(in, ret);

        return ret;
    }

    static void decodeMap(final ByteBuf in,
                          Map<String, String> map) {

        if (map == null) map = new HashMap<>();

        final int size = in.readByte() << 8 & 0xFF00 | in.readByte() & 0xFF;
        if (size > 0) {
            String key, value;
            for (int i = 0; i < size; i++) {
                key = decodeString(in);
                if (!StringUtils.isEmpty(key)) {
                    value = decodeString(in);
                    map.put(key, value);
                } else {
                    break;
                }
            }
        }

    }

}
