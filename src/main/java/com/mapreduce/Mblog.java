package com.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *  CLASS: Mblog
 *  自定义数据类型，实现了Writable 和 DBWritable，将数据传递给Mapper.
 */

public class Mblog implements Writable, DBWritable {
    // id
    private Integer id;
    // uid
    private String uid;
    // event
    private String event;

    //无参构造方法
    public Mblog() {
    }

    //有参构造方法
    public Mblog(Integer id, String uid, String text, String event) {
        this.id = id;
        this.uid = uid;
        this.event = event;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    //实现DBWritable接口要实现的方法
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getInt(1);
        this.uid = resultSet.getString(2);
        this.event = resultSet.getString(4);
    }

    //实现DBWritable接口要实现的方法
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1, this.id);
        preparedStatement.setString(2, this.uid);
        preparedStatement.setString(3, "NULL");
        preparedStatement.setString(4, this.event);
    }

    //实现Writable接口要实现的方法
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.uid = Text.readString(dataInput);
        Text.readString(dataInput);
        this.event = Text.readString(dataInput);
    }

    //实现Writable接口要实现的方法
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        Text.writeString(dataOutput, this.uid);
        Text.writeString(dataOutput, "NULL");
        Text.writeString(dataOutput, this.event);
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((uid == null) ? 0 : uid.hashCode());
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
        Mblog other = (Mblog) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (uid == null) {
            if (other.uid != null)
                return false;
        } else if (!uid.equals(other.uid))
            return false;
        if (event == null) {
            if (other.event != null)
                return false;
        } else if (!event.equals(other.event))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Student [id=" + id + ", uid=" + uid + ", event=" + event + "]";
    }
}
