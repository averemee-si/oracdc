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
