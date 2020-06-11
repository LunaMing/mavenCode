package hadoopPack;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordPair implements WritableComparable<WordPair> {
    private String wordA;
    private String wordB;

    public WordPair(String wordA, String wordB) {
        this.wordA = wordA;
        this.wordB = wordB;
    }


    public String getWordA() {
        return wordA;
    }

    public String getWordB() {
        return wordB;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        wordA = in.readUTF();
        wordB = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeUTF(wordA);
        out.writeUTF(wordB);

    }

    @Override
    public String toString() {
        return wordA + "," + wordB;
    }

    @Override
    public int compareTo(WordPair o) {
        // TODO Auto-generated method stub
        if (this.equals(o))
            return 0;
        else
            return (wordA + wordB).compareTo(o.getWordA() + o.getWordB());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WordPair))
            return false;
        WordPair w = (WordPair) o;
        return (this.wordA.equals(w.wordA) && this.wordA.equals(w.wordB))
                || (this.wordB.contentEquals(w.wordA) && this.wordA.contentEquals(w.wordB));
    }

    public int hashCode() {
        return (wordA.hashCode() + wordB.hashCode()) * 17;
    }

}
