package com.example;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Row;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class RowToBinaryConverter {

    // 根据字段类型将Row中的值转换为字节数组
    public static byte[] convertFieldToBytes(Row row, int fieldIndex, DataType dataType) {
        if (row.isNullAt(fieldIndex)) {
            return handleNullValue(dataType); // 处理空值
        }


        Object value = row.get(fieldIndex);
        try {
            if (dataType instanceof IntegerType) {
                return intToByteArray((Integer) value);
            } else if (dataType instanceof StringType) {
                return stringToByteArray((String) value);
            } else if (dataType instanceof LongType) {
                return longToByteArray((Long) value);
            } else if (dataType instanceof DoubleType) {
                return doubleToByteArray((Double) value);
            } else if (dataType instanceof BooleanType) {
                return booleanToByteArray((Boolean) value);
            } else {
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
            }
        } catch (ClassCastException e) {
            throw new IllegalStateException("字段值类型与Schema不匹配: 字段 " + fieldIndex
                    + " 预期类型 " + dataType + "，实际类型 " + value.getClass());
        }
    }

    // ----------- 具体类型转换方法 -----------
    private static byte[] intToByteArray(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        return buffer.array();
    }

    private static byte[] stringToByteArray(String value) {
        byte[] strBytes = value.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + strBytes.length);
        buffer.putInt(strBytes.length); // 长度前缀
        buffer.put(strBytes);
        return buffer.array();
    }

    private static byte[] longToByteArray(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(value);
        return buffer.array();
    }

    private static byte[] doubleToByteArray(double value) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putDouble(value);
        return buffer.array();
    }

    private static byte[] booleanToByteArray(boolean value) {
        return new byte[] { (byte) (value ? 1 : 0) };
    }

    private static byte[] handleNullValue(DataType dataType) {
        // 生产环境根据需求定制：填充默认值或抛出异常
        throw new IllegalArgumentException("字段不允许为Null");
    }
}
