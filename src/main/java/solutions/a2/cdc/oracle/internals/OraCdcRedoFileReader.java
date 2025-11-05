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

package solutions.a2.cdc.oracle.internals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;

public class OraCdcRedoFileReader implements OraCdcRedoReader {

	private InputStream is;
	private final String redoLog;
	private final int blockSize;

	OraCdcRedoFileReader(final String redoLog, final int blockSize) throws SQLException {
		try {
			is = Files.newInputStream(Paths.get(redoLog), StandardOpenOption.READ);
			if (is.skip(blockSize) != blockSize) {
				throw new SQLException("Unable to skip " + blockSize + " bytes!");
			}
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
		this.redoLog = redoLog;
		this.blockSize = blockSize;
	}

	@Override
	public int read(byte b[], int off, int len) throws SQLException {
		try {
			return is.read(b, off, len);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}
	
	@Override
	public long skip(long n) throws SQLException {
		try {
			return is.skip(n * blockSize);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void close() throws SQLException {
		try {
			is.close();
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void reset() throws SQLException {
		try {
			is.close();
			is = null;
			is = Files.newInputStream(Paths.get(redoLog), StandardOpenOption.READ);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public int blockSize() {
		return blockSize;
	}

	@Override
	public String redoLog() {
		return redoLog;
	}

}
