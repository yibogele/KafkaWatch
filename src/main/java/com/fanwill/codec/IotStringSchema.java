package com.fanwill.codec;

import com.fanwill.model.InData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * Author: Will Fan
 * Created: 2019/10/10 11:40
 * Description:
 */
public class IotStringSchema implements DeserializationSchema<InData>,
        SerializationSchema<InData> {
    private static final long serialVersionUID = 1L;

    /**
     * The charset to use to convert between strings and bytes.
     * The field is transient because we serialize a different delegate object instead
     */
    private transient Charset charset;

    public IotStringSchema() {
        this(StandardCharsets.UTF_8);
    }

    public IotStringSchema(Charset charset) {
        this.charset = checkNotNull(charset);
    }

    @Override
    public InData deserialize(byte[] message) throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/venusmessage.avsc"));

        DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
        GenericRecord payload2 = reader.read(null, decoder);
        if (payload2.get("dateTime").toString().trim().isEmpty()) {
            // some iot device just dont have any fucking ts,
            // so we put in current ts, maybe several ms later than event time
            System.out.println(payload2);

            payload2.put("dateTime", System.currentTimeMillis());
        }

        return new InData(payload2.get("deviceId").toString(),
                payload2.get("productKey").toString(),
                payload2.get("dataType").toString(),
                Long.valueOf(payload2.get("dateTime").toString()),
                (long) message.length);

    }

    /*
        private Tuple4<String, String, String, Long> deserializeInternal(byte[] message) {
            final ByteBuf bb = Unpooled.wrappedBuffer(message);
            Map<String, String> resultMap = new HashMap<>();

            byte[] devidbs = new byte[Constants.DEV_ID_LEN];
            bb.readBytes(devidbs);
            String devidStr = new String(devidbs);

            byte[] devTypebs = new byte[Constants.DEV_TYPE_LEN];
            bb.readBytes(devTypebs);
            final String devTypeStr = new String(devTypebs);

            byte messageType = bb.readByte();

            long messageTime = bb.readLong();

            return new Tuple4<>(devidStr, devTypeStr, messageType, messageTime);

    //        resultMap.put("devidStr", devidStr);
    //        resultMap.put("devTypeStr", devTypeStr);
    //        resultMap.put("messageType", String.valueOf(messageType));
    //        resultMap.put("dataTime", String.valueOf(messageTime));

    //        Decoder.decodeMap(bb, resultMap);
    //
    //        return resultMap.toString();
        }
    */
    @Override
    public boolean isEndOfStream(InData nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(InData element) {
        return element.toString().getBytes(charset);
    }

    @Override
    public TypeInformation<InData> getProducedType() {
        return TypeInformation.of(new TypeHint<InData>() {
        });

    }

    public Charset getCharset() {
        return charset;
    }

    // ------------------------------------------------------------------------
    //  Java Serialization
    // ------------------------------------------------------------------------

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(charset.name());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }
}
