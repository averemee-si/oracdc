/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.internals;

import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_1170;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.SQL_STATE_FILE_NOT_FOUND;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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
		} catch (NoSuchFileException nsfe) {
			throw new SQLException(nsfe.getFile(), SQL_STATE_FILE_NOT_FOUND, ORA_1170, nsfe);
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
