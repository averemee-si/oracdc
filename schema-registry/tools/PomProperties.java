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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipFile;

/**
 * Writes META-INF/maven/&lt;groupId&gt;/&lt;artifactId&gt;/pom.properties for every bundled jar that
 * carries none (Gradle and Bazel builds), so vulnerability scanners can identify it inside the
 * uber jar. Coordinates come from the jar's place in the local Maven repository.
 *
 * Usage: java PomProperties.java &lt;local repository&gt; &lt;output directory&gt; &lt;classpath&gt;
 */
public class PomProperties {

	public static void main(String[] args) throws IOException {
		final Path repository = Path.of(args[0]).toAbsolutePath().normalize();
		final Path output = Path.of(args[1]);
		int written = 0;
		for (final String entry : args[2].split(File.pathSeparator)) {
			if (entry.isBlank() || !entry.endsWith(".jar"))
				continue;
			final Path jar = Path.of(entry).toAbsolutePath().normalize();
			if (hasPomProperties(jar))
				continue;
			// <repository>/<group path>/<artifactId>/<version>/<artifactId>-<version>[-classifier].jar
			final Path relative = repository.relativize(jar);
			final int names = relative.getNameCount();
			if (relative.startsWith("..") || names < 4)
				throw new IllegalStateException(jar + " is not in the local repository " + repository);
			final String version = relative.getName(names - 2).toString();
			final String artifactId = relative.getName(names - 3).toString();
			final String groupId = relative.subpath(0, names - 3).toString().replace(File.separatorChar, '.');
			final Path file = output.resolve("META-INF/maven").resolve(groupId).resolve(artifactId).resolve("pom.properties");
			Files.createDirectories(file.getParent());
			Files.writeString(file, "groupId=" + groupId + "\nartifactId=" + artifactId + "\nversion=" + version + "\n");
			System.out.println("[PomProperties] " + groupId + ":" + artifactId + ":" + version);
			written++;
		}
		System.out.println("[PomProperties] " + written + " pom.properties written to " + output);
	}

	private static boolean hasPomProperties(final Path jar) throws IOException {
		try (ZipFile zip = new ZipFile(jar.toFile())) {
			return zip.stream().anyMatch(e -> e.getName().startsWith("META-INF/maven/") && e.getName().endsWith("/pom.properties"));
		}
	}
}
