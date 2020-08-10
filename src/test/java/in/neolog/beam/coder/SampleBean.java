/*
 * Copyright Adarsh Soodan, 2020
 * Licensed under http://www.apache.org/licenses/LICENSE-2.0
 */
package in.neolog.beam.coder;

import java.io.Serializable;
import java.util.Arrays;

public class SampleBean implements Serializable {

    private static final long serialVersionUID = 1L;

    private int               x;
    private String            str;
    private byte[]            bytes;

    public SampleBean() {
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bytes);
        result = prime * result + ((str == null) ? 0 : str.hashCode());
        result = prime * result + x;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SampleBean other = (SampleBean) obj;
        if (!Arrays.equals(bytes, other.bytes))
            return false;
        if (str == null) {
            if (other.str != null)
                return false;
        } else if (!str.equals(other.str))
            return false;
        if (x != other.x)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SampleBean [x=" + x + ", str=" + str + ", bytes=" + Arrays.toString(bytes) + "]";
    }

}
