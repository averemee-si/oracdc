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

package eu.solutions.a2.cdc.oracle.utils.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

/**
 * 
 * @author averemee
 *
 */
public class OpenFileCheckerNio implements OpenFileChecker {

	@Override
	public boolean isLocked(final String fileName) throws IOException {
		final File file = new File(fileName); 
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel channel = raf.getChannel();
		FileLock lock = channel.lock();
		boolean isLocked = false;
		try {
			lock = channel.tryLock();
		} catch (OverlappingFileLockException e) {
			isLocked = true;
		} finally {
			lock.release();
			lock.close();
			lock = null;
		}
		channel.close();
		channel = null;
		raf.close();
		raf = null;
		return isLocked;
	}

}
