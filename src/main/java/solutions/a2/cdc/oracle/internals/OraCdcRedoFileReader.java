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

public class OraCdcRedoFileReader implements OraCdcRedoReader{

	private final InputStream is;

	OraCdcRedoFileReader(final String redoLog) throws IOException {
		is = Files.newInputStream(Paths.get(redoLog), StandardOpenOption.READ);
	}

	@Override
	public int read(byte b[], int off, int len) throws IOException {
		return is.read(b, off, len);
	}
	
	@Override
	public long skip(long n) throws IOException {
		return is.skip(n);
	}

	@Override
	public void close() throws IOException {
		is.close();
	}

}
