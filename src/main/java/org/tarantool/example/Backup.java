package org.tarantool.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.text.DecimalFormat;
import java.util.Arrays;

import org.tarantool.core.cmd.Delete;
import org.tarantool.core.cmd.Insert;
import org.tarantool.core.cmd.Update;
import org.tarantool.snapshot.Const;
import org.tarantool.snapshot.ReplicationClient;
import org.tarantool.snapshot.XLogReader;
import org.tarantool.snapshot.XLogReader.XLogEntry;
import org.tarantool.snapshot.XLogWriter;

/**
 * Backup tool
 * 
 * @author dgreen
 * 
 */
public class Backup {
	protected DecimalFormat xlogNameFormat = new DecimalFormat("00000000000000000000");
	protected String folder;
	protected FileChannel xlogChannel;
	protected int row;
	protected int limit = 50000;
	protected long lsn = 0L;
	protected ReplicationClient client;
	protected XLogWriter writer;

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public Backup(String folder, String host, int port) throws IOException {
		this.folder = folder;

	}

	protected void getLatestLSN(String folder) throws IOException, FileNotFoundException {
		final File backupFolder = new File(folder);
		String[] xlogs = backupFolder.list(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".xlog");
			}
		});
		boolean hasLogs = xlogs != null && xlogs.length > 0;
		if (hasLogs) {
			Arrays.sort(xlogs);
			XLogReader reader = new XLogReader(new FileInputStream(folder + "/" + xlogs[xlogs.length - 1]).getChannel());
			XLogEntry xlogEntry = null;
			while ((xlogEntry = reader.nextEntry()) != null) {
				lsn = xlogEntry.header.lsn;
			}
			reader.close();
		}
	}

	public void start() throws IOException {
		getLatestLSN(folder);
		System.out.println("Planning to start from lsn: " + lsn);
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					synchronized (this) {
						close();
					}
				} catch (IOException e) {
					throw new IllegalStateException("Can't close xlog", e);
				}
			}
		}));

		final ByteBuffer rowStartMarker = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(Const.ROW_START_MARKER);
		client = new ReplicationClient(SocketChannel.open(new InetSocketAddress("127.0.0.1", 33016)), lsn + 1L) {

			@Override
			protected ByteBuffer readBody(Header header) throws IOException {
				if (Backup.this.xlogChannel == null) {
					Backup.this.xlogChannel = nextFile(folder);
				}
				ByteBuffer body = super.readBody(header);
				this.header.flip();
				rowStartMarker.flip();
				synchronized (Backup.this) {
					while (rowStartMarker.hasRemaining())
						Backup.this.xlogChannel.write(rowStartMarker);
					while (this.header.hasRemaining())
						Backup.this.xlogChannel.write(this.header);
					while (body.hasRemaining())
						Backup.this.xlogChannel.write(body);
					Backup.this.xlogChannel.force(false);
					body.flip();
				}
				return body;
			}

		};

	}

	public XLogEntry nextEntry() throws IOException {
		XLogEntry entry = client.nextEntry();
		lsn = entry.header.lsn;
		if (++row >= limit) {
			close();
			xlogChannel = nextFile(folder);
			row = 0;
		}
		return entry;
	}

	protected FileChannel nextFile(String folder) throws IOException {
		String fileName = folder + "/" + xlogNameFormat.format(lsn + 1L) + ".xlog";
		new File(fileName).createNewFile();
		FileChannel channel = new FileOutputStream(fileName, true).getChannel();
		writer = new XLogWriter(channel);
		return channel;
	}

	public void close() throws IOException {
		if (writer != null) {
			writer.close();
		}
	}

	public static void main(String[] args) throws IOException {
		final Backup backup = new Backup("/home/dgreen/backup", "localhost", 33016);
		backup.start();
		XLogEntry entry = null;
		while ((entry = backup.nextEntry()) != null) {
			StringBuilder pk = new StringBuilder();
			for (int i = 0; i < entry.tuple.size(); i++) {
				if (pk.length() > 0) {
					pk.append(" - ");
				}
				switch (entry.tuple.getBytes(i).length) {
				case 4:
					pk.append(String.valueOf(entry.tuple.getInt(i)));
					break;
				case 8:
					pk.append(String.valueOf(entry.tuple.getLong(i)));
					break;
				default:
					pk.append(entry.tuple.getString(i, "UTF-8"));
				}

			}
			switch (entry.op) {
			case Update.OP_CODE:
				System.out.println("Got update on #" + pk.toString());
				break;
			case Insert.OP_CODE:
				System.out.println("Got insert " + pk.toString());
				break;
			case Delete.OP_CODE:
				System.out.println("Got delete of #" + pk.toString());
				break;
			default:
				System.out.println("Got unknown op " + entry.op + " " + pk.toString());
				break;
			}

		}
	}
}
