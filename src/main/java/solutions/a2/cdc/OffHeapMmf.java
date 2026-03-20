/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package solutions.a2.cdc;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
@SuppressWarnings("preview")
public class OffHeapMmf {

	private static final int END_OF_SEGMENT_MARKER = 0xFFFFFFFF;
	private static final int MADV_HUGEPAGE = 14;
	private static final Logger LOGGER = LoggerFactory.getLogger(OffHeapMmf.class);

	public static final boolean LINUX;
	static {
		LINUX = Strings.CI.endsWith(System.getProperty("os.name"), "inux");
	}

	private static final MethodHandle MADVISE_HANDLE;
	private static final boolean USE_HUGEPAGES;
	static {
		if (Strings.CI.endsWith(System.getProperty("os.name"), "inux")) {
			var linker = Linker.nativeLinker();
			var libc = linker.defaultLookup();
			MADVISE_HANDLE = linker.downcallHandle(
					libc.find("madvise").orElseThrow(),
					FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));
			USE_HUGEPAGES = true;
			LOGGER.info("OffHeapMmf will use HugePages in {} OS", System.getProperty("os.name"));
		} else {
			MADVISE_HANDLE = null;
			USE_HUGEPAGES = false;
		}
	}

	private final List<MemorySegment> segments;
	private final List<Arena> arenas;
	private final File file;
	private final int segmentSize;
	private long writeOffset = 0l;
	private long readOffset = 0;
	private int readIndex = 0;
 
	public OffHeapMmf(final String fileName, final int segmentSize) throws IOException {
		segments = new ArrayList<>();
		arenas = new ArrayList<>();
		file = Path.of(fileName).toFile(); 
		this.segmentSize = segmentSize;
		addSegment();
	}

	private void addSegment() throws IOException {
		var arena = Arena.ofShared();
		var startPosition = (long) segments.size() * segmentSize;
		try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
			raf.setLength(startPosition + segmentSize);
			var channel = raf.getChannel();
			var segment = channel.map(READ_WRITE, startPosition, segmentSize, arena);
			if (USE_HUGEPAGES) {
				try {
					var result = (int) MADVISE_HANDLE.invokeExact(segment, (long) segmentSize, MADV_HUGEPAGE);
					if (result != 0) {
						LOGGER.error(
								"""
								
								=====================
								madvise returned code {} when called for segmentSize = {}
								=====================
								
								""", result, segmentSize);
					}
				} catch (Throwable t) {
					throw new IOException(t);
				}
			}
			segments.add(segment);
			arenas.add(arena);
		}
	}

	public void writeRecord(byte[] data) throws IOException {
		var required = Integer.BYTES + data.length;
		MemorySegment current = segments.get(segments.size() - 1);
		if (writeOffset + required > current.byteSize()) {
			if (writeOffset + 4 <= current.byteSize()) {
				current.set(JAVA_INT, writeOffset, END_OF_SEGMENT_MARKER);
			}
			addSegment();
			current = segments.get(segments.size() - 1);
			writeOffset = 0;
		}
		current.set(JAVA_INT, writeOffset, data.length);
		MemorySegment.copy(data, 0, current, JAVA_BYTE, writeOffset + 4, data.length);
		writeOffset += required;
	}

	public byte[] readNext() {
		if (readIndex >= segments.size())
			return null;
		var current = segments.get(readIndex);
		if (readOffset + Integer.BYTES > current.byteSize()) {
			readIndex++;
			readOffset = 0;
			return readNext();
		}
		var length = current.get(JAVA_INT, readOffset);
		if (length == END_OF_SEGMENT_MARKER) {
			readIndex++;
			readOffset = 0;
			return readNext();
		}
		if (readOffset + Integer.BYTES + length > current.byteSize()) {
			readIndex++;
			readOffset = 0;
			return readNext();
		}
		if (length == 0)
			return null;
		var data = new byte[length];
		MemorySegment.copy(current, JAVA_BYTE, readOffset + Integer.BYTES, data, 0, length);
		readOffset += (Integer.BYTES + length);
		return data;
	}

	public int readIndex() {
		return readIndex;
	}

	public void resetReadIndex() {
		readIndex = 0;
		readOffset = 0;
	}

	public void close() throws IOException {
		for (var i = 0; i < arenas.size(); i++) {
			arenas.get(i).close();
		}
		Files.delete(file.toPath());
	}

}
