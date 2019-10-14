package com.fan.codec;

/**
 * Author: Will Fan
 * Created: 2019/10/10 12:38
 * Description:
 */
public final class Constants {
    static final int    INT_LEN                 = 2;

    static final String CHAR_SET_NAME           = "UTF-8";

    private static byte         PROTOCOL_VERSION_LEN    = 1;

    private static byte         MSG_TYPE_LEN            = 1;

    private static byte         RETAIN_LEN              = 1;

    static byte         DEV_ID_LEN              = 12;

    static byte         DEV_TYPE_LEN            = 8;

    private static byte         MSG_ID_LEN              = 32;

    private static byte         MSG_LEN_LEN             = 2;

    public static int          PROTECOL_HEADER_LEN     = PROTOCOL_VERSION_LEN
            + MSG_TYPE_LEN
            + RETAIN_LEN
            + DEV_ID_LEN
            + DEV_TYPE_LEN
            + MSG_ID_LEN
            + MSG_LEN_LEN;

    public static final char  DEVICE_ID_PREFIX        = 'D';
    public static final char  SYSTEM_ID_PREFIX        = 'S';
}
