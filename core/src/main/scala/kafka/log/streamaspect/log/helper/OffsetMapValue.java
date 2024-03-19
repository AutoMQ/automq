package kafka.log.streamaspect.log.helper;



/**
 * @author ipsum-0320
 */
public class OffsetMapValue {
    private long offset;
    private String line;

    public OffsetMapValue(long offset, String line) {
        this.offset = offset;
        this.line = line;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }
}
