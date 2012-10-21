package org.tarantool.example;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.tarantool.core.Tuple;
import org.tarantool.core.impl.SocketChannelTarantoolConnection;
import org.tarantool.snapshot.SnapshotWriter;

public class TableToSnap {
	/**
	 * convert table to snap table structure is 1 username email@domain.tld 1
	 * 2012-10-14 01:27:05 int varchar varchar tinyint datetime
	 * 
	 * @param args
	 * @throws IOException
	 */
	public void main(String[] args) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream("/tmp/user.gz")), "utf-8"));
		SnapshotWriter writer = new SnapshotWriter(new FileOutputStream("/tmp/user.snap").getChannel());
		String line = null;
		DateFormat indf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		DateFormat outdf = new SimpleDateFormat("yyyyMMddhhmmss");
		Pattern pattern = Pattern.compile("\t");
		while ((line = reader.readLine()) != null) {
			try {
				String[] values = pattern.split(line);
				if (values.length == 5) {
					// 1 username email@domain.tld 1 2012-10-14 01:27:05
					Integer id = Integer.parseInt(values[0]);
					String username = values[1];
					String email = values[2];
					byte[] enabled = { Byte.valueOf(values[3]) };
					Long registered = Long.parseLong(outdf.format(indf.parse(values[4])));
					Tuple tuple = new Tuple(5).setInt(0, id).setString(1, username, "UTF-8").setString(2, email, "UTF-8").setBytes(3, enabled)
							.setLong(4, registered);

					writer.writeRow(0, tuple);
				} else {
					System.err.println("Line should be splited in 5 parts, but has " + values.length + " for " + line);
				}
			} catch (Exception e) {
				System.err.println("Can't parse line " + line);
				e.printStackTrace();
			}
		}
		writer.close();
		reader.close();
		SocketChannelTarantoolConnection con = new SocketChannelTarantoolConnection();
		Tuple t = con.findOne(0, 0, 0, new Tuple(1).setInt(0, 1));
		System.out.println(t.getLong(4));
		con.close();
	}

}
