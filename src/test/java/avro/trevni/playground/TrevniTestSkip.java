package avro.trevni.playground;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnFileWriter;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ColumnValues;
import org.apache.trevni.ValueType;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;

public class TrevniTestSkip {
	@Test
	public void readColumnValues() throws Exception {

		String writeTrevniFile = System.getProperty("WRTIE_TRV", "no");

		String jsonFilePath = "target/electricPowerUsageGenerator.json";
		String trevniFilePath = "target/electricPowerUsageGenerator.trv";

		// write trevni file via json.
		if ("yes".equals(writeTrevniFile)) {
			File f = new File(trevniFilePath);
			if (f.exists())
				f.delete();
			writeTrevniFileFromJson(jsonFilePath, trevniFilePath);
		}

		// read json file.
		System.out.println("readJson");
		long start = System.nanoTime();
		List<Long> timestampList = readJson(jsonFilePath);
		long count = 0;
		for (Long ts : timestampList) {
			count++;
		}
		System.out.println("elapsed time of json: ["
				+ (System.nanoTime() - start) / 1000000
				+ "]ms, total record count: [" + count + "]");

		// read column values from trevni file.
		System.out.println("readTrevni");
		long startTrevni = System.nanoTime();
		// ColumnFileReader in = new ColumnFileReader(new File(trevniFilePath));
		// Iterator<Long> timestampListFromTrevni = in.getValues("timestamp");

		Iterator<Long> timestampListFromTrevni = readTrevni(trevniFilePath,
				"timestamp");
		count = 0;
		while (timestampListFromTrevni.hasNext()) {
			long ts = timestampListFromTrevni.next();
			count++;
		}
		System.out.println("elapsed time of trevni: ["
				+ (System.nanoTime() - startTrevni) / 1000000
				+ "]ms, total record count: [" + count + "]");
	}

	private ColumnValues readTrevni(String trevniFilePath, String column)
			throws Exception {
		ColumnFileReader in = new ColumnFileReader(new File(trevniFilePath));
		return (ColumnValues) in.getValues(column);
	}

	private void writeTrevniFileFromJson(String jsonFilePath,
			String trevniFilePath) throws IOException, FileNotFoundException,
			JsonParseException, JsonMappingException {
		ColumnMetaData timestampCM = new ColumnMetaData("timestamp",
				ValueType.LONG);

		ColumnMetaData addressCodeCM = new ColumnMetaData("addressCode",
				ValueType.STRING);

		ColumnMetaData devicePowerEventListCM = new ColumnMetaData(
				"devicePowerEventList", ValueType.NULL);
		devicePowerEventListCM.isArray(true);

		ColumnMetaData powerCM = new ColumnMetaData("power", ValueType.DOUBLE);
		powerCM.setParent(devicePowerEventListCM);

		ColumnMetaData deviceTypeCM = new ColumnMetaData("deviceType",
				ValueType.INT);
		deviceTypeCM.setParent(devicePowerEventListCM);

		ColumnMetaData deviceIdCM = new ColumnMetaData("deviceId",
				ValueType.INT);
		deviceIdCM.setParent(devicePowerEventListCM);

		ColumnMetaData statusCM = new ColumnMetaData("status", ValueType.INT);
		statusCM.setParent(devicePowerEventListCM);

		ColumnFileWriter out = new ColumnFileWriter(createFileMeta(),
				timestampCM, addressCodeCM, devicePowerEventListCM, powerCM,
				deviceTypeCM, deviceIdCM, statusCM);

		ObjectMapper mapper = new ObjectMapper();

		File jsonFile = new File(jsonFilePath);

		BufferedReader br = new BufferedReader(new FileReader(jsonFile));
		String line;
		while ((line = br.readLine()) != null) {
			ElectricPowerUsage electricPowerUsage = mapper.readValue(line,
					new TypeReference<ElectricPowerUsage>() {
					});
			for (DevicePowerEvent event : electricPowerUsage
					.getDevicePowerEventList()) {
				out.writeRow(electricPowerUsage.getTimestamp(),
						electricPowerUsage.getAddressCode(), null,
						event.getPower(), event.getDeviceType(),
						event.getDeviceId(), event.getStatus());
			}
		}
		br.close();
		out.writeTo(new File(trevniFilePath));

	}

	private ColumnFileMetaData createFileMeta() {
		return new ColumnFileMetaData().setCodec("snappy").setChecksum("crc32");
	}

	private List<Long> readJson(String jsonFilePath)
			throws FileNotFoundException, IOException, JsonParseException,
			JsonMappingException {
		List<Long> timestampList = new ArrayList<Long>();

		ObjectMapper mapper = new ObjectMapper();

		File jsonFile = new File(jsonFilePath);

		BufferedReader br = new BufferedReader(new FileReader(jsonFile));
		String line;
		while ((line = br.readLine()) != null) {
			ElectricPowerUsage electricPowerUsage = mapper.readValue(line,
					new TypeReference<ElectricPowerUsage>() {
					});
			timestampList.add(electricPowerUsage.getTimestamp());
		}
		br.close();

		return timestampList;
	}
}
