# Contributing to oracdc

Thank you for your interest in **oracdc**. Bug reports, reproducers, documentation fixes and pull requests are all welcome.

## Contributor License Agreement, required

oracdc is dual-licensed: AGPLv3 for the open-source edition and a commercial license for subscribers. Every contribution therefore has to be covered by the **[oracdc Contributor License Agreement](CLA.md)**.

Accepting takes one comment on your first pull request, the CLA Assistant bot will ask you and explain what to post. **Pull requests cannot be merged until the `CLA` check passes.** If you contribute on behalf of an employer, read [the corporate section of the CLA](CLA.md#contributing-on-behalf-of-an-employer) first: a comment from your personal account may not be enough.

## Code of Conduct

This project follows the [Code of Conduct](CODE_OF_CONDUCT.md). By participating you are expected to uphold it.

## Reporting bugs and requesting features

Open an issue using one of the templates: bug report, feature request, or custom. For bugs, the single most useful thing you can provide is enough detail to reproduce:

* oracdc version, and which connector (`OraCdcRedoMinerConnector`, `OraCdcLogMinerConnector`, `KafkaSourceSnapshotLogConnector`, or a sink/transform)
* Oracle Database version and edition, and whether RAC/ASM/Data Guard/TDE/compression is involved
* JDK version and Apache Kafka / Kafka Connect version
* the connector configuration, **with credentials and hostnames removed**
* the relevant part of the Kafka Connect log, including the messages on connector initialization and the full stack trace
* what you expected to happen, and what happened instead

Please do not paste customer data, redo log contents, or anything else you are not free to publish.

### Security issues

Do **not** open a public issue for a suspected vulnerability. Email **oracle@a2.solutions** with the details and give us a reasonable opportunity to release a fix before disclosure.

## Development setup

oracdc is a Maven project. Java 25 is the minimum and recommended JDK (see [Supported JVM versions](README.adoc)).

```
git clone https://github.com/averemee-si/oracdc.git
cd oracdc
./mvnw clean install -Dgpg.skip
```

Use `./mvnw` (the Maven wrapper) rather than a locally installed Maven, so that everyone builds with the same Maven version. `-Dgpg.skip` skips artifact signing, which is only needed for releases.

Run the tests alone with:

```
./mvnw test
```

## Pull request guidelines

* **Discuss substantial changes first.** For anything beyond a bug fix, new connector options, changes to the redo parsing, dependency upgrades, changes to the on-the-wire or on-disk formats, open an issue before writing the code, so that the design can be agreed. It is disappointing for everyone when a large pull request has to be turned down on architectural grounds.
* **One logical change per pull request.** Do not mix a bug fix with reformatting.
* **Base your work on `master`** and keep the branch up to date with it.
* **Every new or modified source file keeps the AGPL license header** used throughout `src/main/java`, copy it verbatim from an existing file.
* **Match the surrounding code style**: tabs for indentation, the existing brace and import conventions. The Eclipse project settings in `.settings/` reflect the house style.
* **Add tests** under `src/test/java` for bug fixes and new behaviour where it is practical to do so, and make sure `./mvnw clean install -Dgpg.skip` passes before you push.
* **Do not commit generated artifacts**, IDE state beyond what is already tracked, or changes to the project version in `pom.xml`, releases are cut by the maintainers.
* **Write a useful pull request description**: what changes, why, and how you tested it. Reference the issue it fixes (`Fixes #123`).

Note that patches sent by email or pasted into issues cannot be accepted, contributions must arrive as pull requests, because that is where the CLA check runs.

## Review and merge

Pull requests are reviewed by the maintainers. Expect questions and requested changes; this is normal and not a judgement on the work. A pull request is merged when the review is resolved, the build passes, and the `CLA` check is green. Merged pull requests are locked, which preserves the CLA acceptance recorded in them.

## License

By contributing you agree that your contributions are licensed as set out in [CLA.md](CLA.md), and that the project distributes them under the AGPLv3 and under A2 Rešitve d.o.o.'s commercial license.

## Contact

Commercial licensing and support: **sales@a2.solutions** · Technical questions: **oracle@a2.solutions**
