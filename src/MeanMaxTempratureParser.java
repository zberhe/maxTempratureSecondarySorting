import org.apache.hadoop.io.Text;

public class MeanMaxTempratureParser {
    private static final int MISSING_TEMPERATURE = 9999;
    private String date;
    private String station_id;
    private int airTemperature;
    private String quality;
    public void parse(String record) {
        station_id=record.substring(4,15);
        date = record.substring(15, 23);
        String airTemperatureString;
// Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        } else {
            airTemperatureString = record.substring(87, 92);
        }
        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }
    public void parse(Text record) {
        parse(record.toString());
    }
    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }
    public String getDate() {
        return date;
    }
    public String getStation_id() {
        return station_id;
    }
    public int getAirTemperature() {
        return airTemperature;
    }
}
