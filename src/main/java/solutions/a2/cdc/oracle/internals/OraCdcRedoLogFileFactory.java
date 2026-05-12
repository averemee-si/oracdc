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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.SQLException;

import solutions.a2.oracle.utils.BinaryUtils;

public class OraCdcRedoLogFileFactory extends OraCdcRedoLogFactoryBase implements OraCdcRedoLogFactory {


	public OraCdcRedoLogFileFactory(final BinaryUtils bu, final boolean valCheckSum) {
		super(bu, valCheckSum);
	}

	@Override
	public OraCdcRedoLog get(final String redoLog) throws SQLException {
		try {
			InputStream fis = Files.newInputStream(Paths.get(redoLog), StandardOpenOption.READ);
			long[] blockSizeAndCount = blockSizeAndCount(fis, redoLog);		
			fis.close();
			fis = null;
			return new OraCdcRedoLog(
				new OraCdcRedoFileReader(redoLog, (int) blockSizeAndCount[0]),
				valCheckSum,
				bu,
				blockSizeAndCount[1]);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public OraCdcRedoLog get(String redoLog, boolean online, int blockSize, long blockCount) throws SQLException {
		try {
			return new OraCdcRedoLog(
				new OraCdcRedoFileReader(redoLog, blockSize),
				valCheckSum,
				bu,
				blockCount);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void reset() throws SQLException {
	}

	@Override
	public void reset(Connection connection) throws SQLException {
	}

}
