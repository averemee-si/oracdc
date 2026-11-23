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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Writes the schema registry part of LICENSE-3rdPARTIES.txt: every component bundled in the
 * plugin jars, with its version, its license file and the jars that contain it.
 *
 * Components come from the META-INF/maven/**&#47;pom.properties of the built jars (see
 * PomProperties.java), licenses from the POMs as collected by license-maven-plugin. A license
 * other than Apache 2.0 must have a file in LICENSE_FILES below, otherwise the tool stops:
 * a new component under a new license needs a decision and a text in licenses/.
 *
 * Usage, from schema-registry/:
 *   mvn -B package license:aggregate-add-third-party
 *   java tools/ThirdPartyLicenses.java [--update ../LICENSE-3rdPARTIES.txt]
 * Without --update the section is printed; with it, the section replaces the one in the file
 * (from its SECTION_TITLE block to the end) or is appended.
 */
public class ThirdPartyLicenses {

	private static final String THIRD_PARTY = "target/generated-sources/license/THIRD-PARTY.txt";
	private static final String SEPARATOR = "===============================================================================";
	private static final String SECTION_TITLE = "The Docker image (Dockerfile) also bundles four Kafka Connect plugins built from";

	private static final String APACHE = "Apache License Version 2.0";
	private static final String MIT = "MIT License";
	private static final String BSD3 = "BSD-3-Clause";
	private static final String CCL = "Confluent Community License 1.0";
	/** groupId -> license file, for every component not under the Apache License */
	private static final Map<String, String> LICENSE_FILES = Map.of(
			"com.microsoft.kiota", "licenses/MIT-kiota",
			"com.google.protobuf", "licenses/BSD-3-Clause-protobuf",
			"org.antlr", "licenses/BSD-3-Clause-antlr",
			"org.threeten", "licenses/BSD-3-Clause-threeten-extra",
			"com.google.re2j", "licenses/BSD-3-Clause-re2j",
			"io.confluent", "licenses/Confluent-Community-License");
	/** artifactIds too generic to stand alone get their groupId */
	private static final List<String> QUALIFIED_GROUPS = List.of("dev.cel");

	private static final Pattern COORDINATES = Pattern.compile("\\(([^()\\s]+):([^()\\s]+):([^()\\s]+) - [^()]*\\)\\s*$");
	private static final Pattern LEADING_LICENSE = Pattern.compile("^\\s*\\(([^()]+)\\)");
	private static final Pattern PLUGIN_JAR = Pattern.compile("(.+)-schema-client-[0-9.]+\\.jar");

	public static void main(String[] args) throws IOException {
		final Map<String, List<String>> licenses = readLicenses(Path.of(THIRD_PARTY));

		final List<Path> jars;
		try (Stream<Path> files = Files.find(Path.of("."), 4, (p, a) -> {
				final String name = p.getFileName().toString();
				return p.getParent() != null && p.getParent().endsWith("target") && PLUGIN_JAR.matcher(name).matches() && !name.startsWith("original-");
			})) {
			jars = files.sorted(Comparator.comparing(p -> p.getFileName().toString())).toList();
		}
		if (jars.isEmpty())
			throw new IllegalStateException("no plugin jars under */*/target: run mvn package first");

		// groupId:artifactId -> version, plugin jars
		final Map<String, String> versions = new HashMap<>();
		final Map<String, TreeSet<String>> where = new TreeMap<>(Comparator.comparing((String ga) -> ga.substring(ga.indexOf(':') + 1).toLowerCase()).thenComparing(ga -> ga));
		for (final Path jar : jars) {
			final Matcher m = PLUGIN_JAR.matcher(jar.getFileName().toString());
			m.matches();
			final String plugin = m.group(1);
			try (ZipFile zip = new ZipFile(jar.toFile())) {
				for (final ZipEntry e : zip.stream().toList()) {
					if (!e.getName().startsWith("META-INF/maven/") || !e.getName().endsWith("/pom.properties"))
						continue;
					final Properties p = new Properties();
					p.load(zip.getInputStream(e));
					if ("solutions.a2.oracle".equals(p.getProperty("groupId")))
						continue;
					final String ga = p.getProperty("groupId") + ":" + p.getProperty("artifactId");
					versions.put(ga, p.getProperty("version").strip());
					where.computeIfAbsent(ga, k -> new TreeSet<>()).add(plugin);
				}
			}
		}

		final Map<String, List<String>> groups = new LinkedHashMap<>();
		for (final String title : List.of(APACHE, MIT, BSD3, CCL))
			groups.put(title, new ArrayList<>());
		for (final Map.Entry<String, TreeSet<String>> e : where.entrySet()) {
			final String ga = e.getKey();
			final String groupId = ga.substring(0, ga.indexOf(':'));
			final String artifactId = ga.substring(ga.indexOf(':') + 1);
			final List<String> names = licenses.get(ga);
			if (names == null)
				throw new IllegalStateException(ga + " is not in " + THIRD_PARTY + ": rerun license:aggregate-add-third-party");
			final String title = title(ga, names);
			final String file = APACHE.equals(title) ? "licenses/Apache_License" : LICENSE_FILES.get(groupId);
			if (file == null)
				throw new IllegalStateException(ga + " is under " + names + ": add its license text to licenses/ and to LICENSE_FILES");
			final String label = QUALIFIED_GROUPS.contains(groupId) ? ga : artifactId;
			groups.get(title).add("- " + label + "-" + versions.get(ga) + " [" + String.join(", ", e.getValue()) + "], see: " + file);
		}

		final StringBuilder sb = new StringBuilder();
		sb.append(SEPARATOR).append('\n')
			.append(SECTION_TITLE).append('\n')
			.append("schema-registry/ (one uber jar per registry and format, in $KAFKA_HOME/connect/lib):\n");
		for (final Path jar : jars)
			sb.append("  ").append(jar.getFileName()).append('\n');
		sb.append("Each entry below lists the plugin jars that contain the component.\n");
		for (final Map.Entry<String, List<String>> g : groups.entrySet()) {
			if (g.getValue().isEmpty())
				continue;
			sb.append("\n----------------------------------------\n").append(g.getKey()).append('\n');
			if (CCL.equals(g.getKey()))
				sb.append("""

					This software is made available by Confluent, Inc., under the terms of the
					Confluent Community License Agreement, Version 1.0 located at
					http://www.confluent.io/confluent-community-license.  BY INSTALLING,
					DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY OF THE SOFTWARE, YOU AGREE
					TO THE TERMS OF SUCH LICENSE AGREEMENT.
					""");
			sb.append('\n');
			for (final String line : g.getValue())
				sb.append(line).append('\n');
		}
		final String section = sb.toString();

		if (args.length == 2 && "--update".equals(args[0])) {
			final Path target = Path.of(args[1]);
			final String text = Files.readString(target);
			final int title = text.indexOf(SEPARATOR + "\n" + SECTION_TITLE);
			final String head = title < 0 ? text.stripTrailing() + "\n\n" : text.substring(0, title);
			Files.writeString(target, head + section);
			System.out.println((title < 0 ? "appended to " : "updated ") + target + ": " + where.size() + " components in " + jars.size() + " jars");
		} else if (args.length == 0)
			System.out.print(section);
		else
			throw new IllegalArgumentException("usage: java tools/ThirdPartyLicenses.java [--update <LICENSE-3rdPARTIES.txt>]");
	}

	/** THIRD-PARTY.txt lines: "     (License A) (License B) Name (groupId:artifactId:version - url)" */
	private static Map<String, List<String>> readLicenses(final Path file) throws IOException {
		if (!Files.exists(file))
			throw new IllegalStateException(file + " is missing: run mvn license:aggregate-add-third-party");
		final Map<String, List<String>> result = new HashMap<>();
		for (final String line : Files.readAllLines(file)) {
			final Matcher coordinates = COORDINATES.matcher(line);
			if (!coordinates.find())
				continue;
			final List<String> names = new ArrayList<>();
			String rest = line.substring(0, coordinates.start());
			for (Matcher m = LEADING_LICENSE.matcher(rest); m.find(); m = LEADING_LICENSE.matcher(rest)) {
				names.add(m.group(1));
				rest = rest.substring(m.end());
			}
			result.put(coordinates.group(1) + ":" + coordinates.group(2), names);
		}
		return result;
	}

	private static String title(final String ga, final List<String> names) {
		final String all = String.join(" ", names).toLowerCase();
		if (all.contains("confluent community"))
			return CCL;
		if (all.contains("mit"))
			return MIT;
		if (all.contains("bsd") || all.contains("go license"))
			return BSD3;
		if (all.contains("apache"))
			return APACHE;
		throw new IllegalStateException(ga + ": unknown license " + names);
	}
}
