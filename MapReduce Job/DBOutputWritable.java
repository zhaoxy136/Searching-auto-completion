import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by barryzhao on 10/12/16.
 */
public class DBOutputWritable implements DBWritable {

    private String startingPhrase;
    private String followingWord;
    private int count;

    public DBOutputWritable(String startingPhrase, String followingWord, int count) {
        this.startingPhrase = startingPhrase;
        this.followingWord = followingWord;
        this.count = count;
    }

    public void write(PreparedStatement args0) throws SQLException {
        args0.setString(1, startingPhrase);
        args0.setString(2, followingWord);
        args0.setInt(3, count);
    }

    public void readFields(ResultSet args0) throws SQLException {
        this.startingPhrase = args0.getString(1);
        this.followingWord = args0.getString(2);
        this.count = args0.getInt(3);
    }

}
