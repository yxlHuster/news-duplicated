package com.hot.cmt.duplicate;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.thrift.ShortStack;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TTransport;

public class GBKCompactProtocol extends TProtocol {
    private static final TStruct ANONYMOUS_STRUCT = new TStruct("");
    private static final TField TSTOP = new TField("", (byte) 0, (short) 0);

    private static final byte[] ttypeToCompactType = new byte[16];
    private static final byte PROTOCOL_ID = -126;
    private static final byte VERSION = 1;
    private static final byte VERSION_MASK = 31;
    private static final byte TYPE_MASK = -32;
    private static final int TYPE_SHIFT_AMOUNT = 5;
    private ShortStack lastField_ = new ShortStack(15);

    private short lastFieldId_ = 0;

    private TField booleanField_ = null;

    private Boolean boolValue_ = null;

    byte[] i32buf = new byte[5];

    byte[] varint64out = new byte[10];

    private byte[] byteDirectBuffer = new byte[1];

    byte[] byteRawBuf = new byte[1];

    public GBKCompactProtocol(TTransport transport) {
        super(transport);
    }

    public void reset() {
        this.lastField_.clear();
        this.lastFieldId_ = 0;
    }

    public void writeMessageBegin(TMessage message)
            throws TException {
        writeByteDirect((byte) -126);
        writeByteDirect(0x1 | message.type << 5 & 0xFFFFFFE0);
        writeVarint32(message.seqid);
        writeString(message.name);
    }

    public void writeStructBegin(TStruct struct)
            throws TException {
        this.lastField_.push(this.lastFieldId_);
        this.lastFieldId_ = 0;
    }

    public void writeStructEnd()
            throws TException {
        this.lastFieldId_ = this.lastField_.pop();
    }

    public void writeFieldBegin(TField field)
            throws TException {
        if (field.type == 2) {
            this.booleanField_ = field;
        } else writeFieldBeginInternal(field, (byte) -1);
    }

    private void writeFieldBeginInternal(TField field, byte typeOverride)
            throws TException {
        byte typeToWrite = typeOverride == -1 ? getCompactType(field.type) : typeOverride;

        if ((field.id > this.lastFieldId_) && (field.id - this.lastFieldId_ <= 15)) {
            writeByteDirect(field.id - this.lastFieldId_ << 4 | typeToWrite);
        } else {
            writeByteDirect(typeToWrite);
            writeI16(field.id);
        }

        this.lastFieldId_ = field.id;
    }

    public void writeFieldStop()
            throws TException {
        writeByteDirect((byte) 0);
    }

    public void writeMapBegin(TMap map)
            throws TException {
        if (map.size == 0) {
            writeByteDirect(0);
        } else {
            writeVarint32(map.size);
            writeByteDirect(getCompactType(map.keyType) << 4 | getCompactType(map.valueType));
        }
    }

    public void writeListBegin(TList list)
            throws TException {
        writeCollectionBegin(list.elemType, list.size);
    }

    public void writeSetBegin(TSet set)
            throws TException {
        writeCollectionBegin(set.elemType, set.size);
    }

    public void writeBool(boolean b)
            throws TException {
        if (this.booleanField_ != null) {
            writeFieldBeginInternal(this.booleanField_, (byte) (b ? 1 : 2));
            this.booleanField_ = null;
        } else {
            writeByteDirect((byte) (b ? 1 : 2));
        }
    }

    public void writeByte(byte b)
            throws TException {
        writeByteDirect(b);
    }

    public void writeI16(short i16)
            throws TException {
        writeVarint32(intToZigZag(i16));
    }

    public void writeI32(int i32)
            throws TException {
        writeVarint32(intToZigZag(i32));
    }

    public void writeI64(long i64)
            throws TException {
        writeVarint64(longToZigzag(i64));
    }

    public void writeDouble(double dub)
            throws TException {
        byte[] data = {0, 0, 0, 0, 0, 0, 0, 0};
        fixedLongToBytes(Double.doubleToLongBits(dub), data, 0);
        this.trans_.write(data);
    }

    public void writeString(String str) throws TException {
        try {
            byte[] bytes = str.getBytes("GBK");
            writeBinary(bytes, 0, bytes.length);
        } catch (UnsupportedEncodingException e) {
            throw new TException("GBK not supported!");
        }
    }

    public void writeBinary(ByteBuffer bin)
            throws TException {
        int length = bin.limit() - bin.position();
        writeBinary(bin.array(), bin.position() + bin.arrayOffset(), length);
    }

    private void writeBinary(byte[] buf, int offset, int length) throws TException {
        writeVarint32(length);
        this.trans_.write(buf, offset, length);
    }

    public void writeMessageEnd() throws TException {
    }

    public void writeMapEnd() throws TException {
    }

    public void writeListEnd() throws TException {
    }

    public void writeSetEnd() throws TException {
    }

    public void writeFieldEnd() throws TException {
    }

    protected void writeCollectionBegin(byte elemType, int size) throws TException {
        if (size <= 14) {
            writeByteDirect(size << 4 | getCompactType(elemType));
        } else {
            writeByteDirect(0xF0 | getCompactType(elemType));
            writeVarint32(size);
        }
    }

    private void writeVarint32(int n)
            throws TException {
        int idx = 0;
        while (true) {
            if ((n & 0xFFFFFF80) == 0) {
                this.i32buf[(idx++)] = (byte) n;

                break;
            }

            this.i32buf[(idx++)] = (byte) (n & 0x7F | 0x80);

            n >>>= 7;
        }

        this.trans_.write(this.i32buf, 0, idx);
    }

    private void writeVarint64(long n)
            throws TException {
        int idx = 0;
        while (true) {
            if ((n & 0xFFFFFF80) == 0L) {
                this.varint64out[(idx++)] = (byte) (int) n;
                break;
            }
            this.varint64out[(idx++)] = (byte) (int) (n & 0x7F | 0x80);
            n >>>= 7;
        }

        this.trans_.write(this.varint64out, 0, idx);
    }

    private long longToZigzag(long l) {
        return l << 1 ^ l >> 63;
    }

    private int intToZigZag(int n) {
        return n << 1 ^ n >> 31;
    }

    private void fixedLongToBytes(long n, byte[] buf, int off) {
        buf[(off + 0)] = (byte) (int) (n & 0xFF);
        buf[(off + 1)] = (byte) (int) (n >> 8 & 0xFF);
        buf[(off + 2)] = (byte) (int) (n >> 16 & 0xFF);
        buf[(off + 3)] = (byte) (int) (n >> 24 & 0xFF);
        buf[(off + 4)] = (byte) (int) (n >> 32 & 0xFF);
        buf[(off + 5)] = (byte) (int) (n >> 40 & 0xFF);
        buf[(off + 6)] = (byte) (int) (n >> 48 & 0xFF);
        buf[(off + 7)] = (byte) (int) (n >> 56 & 0xFF);
    }

    private void writeByteDirect(byte b)
            throws TException {
        this.byteDirectBuffer[0] = b;
        this.trans_.write(this.byteDirectBuffer);
    }

    private void writeByteDirect(int n)
            throws TException {
        writeByteDirect((byte) n);
    }

    public TMessage readMessageBegin()
            throws TException {
        byte protocolId = readByte();
        if (protocolId != -126) {
            throw new TProtocolException("Expected protocol id " + Integer.toHexString(-126) + " but got " + Integer.toHexString(protocolId));
        }
        byte versionAndType = readByte();
        byte version = (byte) (versionAndType & 0x1F);
        if (version != 1) {
            throw new TProtocolException("Expected version 1 but got " + version);
        }
        byte type = (byte) (versionAndType >> 5 & 0x3);
        int seqid = readVarint32();
        String messageName = readString();
        return new TMessage(messageName, type, seqid);
    }

    public TStruct readStructBegin()
            throws TException {
        this.lastField_.push(this.lastFieldId_);
        this.lastFieldId_ = 0;
        return ANONYMOUS_STRUCT;
    }

    public void readStructEnd()
            throws TException {
        this.lastFieldId_ = this.lastField_.pop();
    }

    public TField readFieldBegin()
            throws TException {
        byte type = readByte();

        if (type == 0) {
            return TSTOP;
        }

        short modifier = (short) ((type & 0xF0) >> 4);
        short fieldId;
        if (modifier == 0) {
            fieldId = readI16();
        } else {
            fieldId = (short) (this.lastFieldId_ + modifier);
        }

        TField field = new TField("", getTType((byte) (type & 0xF)), fieldId);

        if (isBoolType(type)) {
            this.boolValue_ = ((byte) (type & 0xF) == 1 ? Boolean.TRUE : Boolean.FALSE);
        }

        this.lastFieldId_ = field.id;
        return field;
    }

    public TMap readMapBegin()
            throws TException {
        int size = readVarint32();
        byte keyAndValueType = size == 0 ? 0 : readByte();
        return new TMap(getTType((byte) (keyAndValueType >> 4)), getTType((byte) (keyAndValueType & 0xF)), size);
    }

    public TList readListBegin()
            throws TException {
        byte size_and_type = readByte();
        int size = size_and_type >> 4 & 0xF;
        if (size == 15) {
            size = readVarint32();
        }
        byte type = getTType(size_and_type);
        return new TList(type, size);
    }

    public TSet readSetBegin()
            throws TException {
        return new TSet(readListBegin());
    }

    public boolean readBool()
            throws TException {
        if (this.boolValue_ != null) {
            boolean result = this.boolValue_.booleanValue();
            this.boolValue_ = null;
            return result;
        }
        return readByte() == 1;
    }

    public byte readByte()
            throws TException {
        byte b;
        if (this.trans_.getBytesRemainingInBuffer() > 0) {
            b = this.trans_.getBuffer()[this.trans_.getBufferPosition()];
            this.trans_.consumeBuffer(1);
        } else {
            this.trans_.readAll(this.byteRawBuf, 0, 1);
            b = this.byteRawBuf[0];
        }
        return b;
    }

    public short readI16()
            throws TException {
        return (short) zigzagToInt(readVarint32());
    }

    public int readI32()
            throws TException {
        return zigzagToInt(readVarint32());
    }

    public long readI64()
            throws TException {
        return zigzagToLong(readVarint64());
    }

    public double readDouble()
            throws TException {
        byte[] longBits = new byte[8];
        this.trans_.readAll(longBits, 0, 8);
        return Double.longBitsToDouble(bytesToLong(longBits));
    }

    public String readString()
            throws TException {
        int length = readVarint32();

        if (length == 0) {
            return "";
        }
        try {
            if (this.trans_.getBytesRemainingInBuffer() >= length) {
                String str = new String(this.trans_.getBuffer(), this.trans_.getBufferPosition(), length, "GBK");
                this.trans_.consumeBuffer(length);
                return str;
            }
            return new String(readBinary(length), "GBK");
        } catch (UnsupportedEncodingException e) {
        }
        throw new TException("GBK not supported!");
    }

    public ByteBuffer readBinary()
            throws TException {
        int length = readVarint32();
        if (length == 0) return ByteBuffer.wrap(new byte[0]);

        byte[] buf = new byte[length];
        this.trans_.readAll(buf, 0, length);
        return ByteBuffer.wrap(buf);
    }

    private byte[] readBinary(int length)
            throws TException {
        if (length == 0) return new byte[0];

        byte[] buf = new byte[length];
        this.trans_.readAll(buf, 0, length);
        return buf;
    }

    public void readMessageEnd() throws TException {
    }

    public void readFieldEnd() throws TException {
    }

    public void readMapEnd() throws TException {
    }

    public void readListEnd() throws TException {
    }

    public void readSetEnd() throws TException {
    }

    private int readVarint32() throws TException {
        int result = 0;
        int shift = 0;
        if (this.trans_.getBytesRemainingInBuffer() >= 5) {
            byte[] buf = this.trans_.getBuffer();
            int pos = this.trans_.getBufferPosition();
            int off = 0;
            while (true) {
                byte b = buf[(pos + off)];
                result |= (b & 0x7F) << shift;
                if ((b & 0x80) != 128) break;
                shift += 7;
                off++;
            }
            this.trans_.consumeBuffer(off + 1);
        } else {
            while (true) {
                byte b = readByte();
                result |= (b & 0x7F) << shift;
                if ((b & 0x80) != 128) break;
                shift += 7;
            }
        }
        return result;
    }

    private long readVarint64()
            throws TException {
        int shift = 0;
        long result = 0L;
        if (this.trans_.getBytesRemainingInBuffer() >= 10) {
            byte[] buf = this.trans_.getBuffer();
            int pos = this.trans_.getBufferPosition();
            int off = 0;
            while (true) {
                byte b = buf[(pos + off)];
                result |= b & 0x7F << shift;
                if ((b & 0x80) != 128) break;
                shift += 7;
                off++;
            }
            this.trans_.consumeBuffer(off + 1);
        } else {
            while (true) {
                byte b = readByte();
                result |= b & 0x7F << shift;
                if ((b & 0x80) != 128) break;
                shift += 7;
            }
        }
        return result;
    }

    private int zigzagToInt(int n) {
        return n >>> 1 ^ -(n & 0x1);
    }

    private long zigzagToLong(long n) {
        return n >>> 1 ^ -(n & 1L);
    }

    private long bytesToLong(byte[] bytes) {
        return (bytes[7] & 0xFF) << 56 | (bytes[6] & 0xFF) << 48 | (bytes[5] & 0xFF) << 40 | (bytes[4] & 0xFF) << 32 | (bytes[3] & 0xFF) << 24 | (bytes[2] & 0xFF) << 16 | (bytes[1] & 0xFF) << 8 | bytes[0] & 0xFF;
    }

    private boolean isBoolType(byte b) {
        int lowerNibble = b & 0xF;
        return (lowerNibble == 1) || (lowerNibble == 2);
    }

    private byte getTType(byte type)
            throws TProtocolException {
        switch ((byte) (type & 0xF)) {
            case 0:
                return 0;
            case 1:
            case 2:
                return 2;
            case 3:
                return 3;
            case 4:
                return 6;
            case 5:
                return 8;
            case 6:
                return 10;
            case 7:
                return 4;
            case 8:
                return 11;
            case 9:
                return 15;
            case 10:
                return 14;
            case 11:
                return 13;
            case 12:
                return 12;
        }
        throw new TProtocolException("don't know what type: " + (byte) (type & 0xF));
    }

    private byte getCompactType(byte ttype) {
        return ttypeToCompactType[ttype];
    }

    static {
        ttypeToCompactType[0] = 0;
        ttypeToCompactType[2] = 1;
        ttypeToCompactType[3] = 3;
        ttypeToCompactType[6] = 4;
        ttypeToCompactType[8] = 5;
        ttypeToCompactType[10] = 6;
        ttypeToCompactType[4] = 7;
        ttypeToCompactType[11] = 8;
        ttypeToCompactType[15] = 9;
        ttypeToCompactType[14] = 10;
        ttypeToCompactType[13] = 11;
        ttypeToCompactType[12] = 12;
    }

    private static class Types {
        public static final byte BOOLEAN_TRUE = 1;
        public static final byte BOOLEAN_FALSE = 2;
        public static final byte BYTE = 3;
        public static final byte I16 = 4;
        public static final byte I32 = 5;
        public static final byte I64 = 6;
        public static final byte DOUBLE = 7;
        public static final byte BINARY = 8;
        public static final byte LIST = 9;
        public static final byte SET = 10;
        public static final byte MAP = 11;
        public static final byte STRUCT = 12;
    }

    public static class Factory implements TProtocolFactory {
        public TProtocol getProtocol(TTransport trans) { return new GBKCompactProtocol(trans); }
    }
}
