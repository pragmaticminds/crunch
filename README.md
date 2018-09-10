# Readme
Please read this before asking questions!!!

## Documentation

Doku is made in markdown and is automatically deployt to:
http://crunch.pragmaticindustries.de/

New files are placed in
```
src/site/markdwon/...
```
If new sections are defined please add them in the `site.xml` and refer to the files as `.html` as they will be compiled automatically.

## Other documents
### Modules

[Crunch API](api/README.MD)

## Release
As this is a "lib" project which is used by other projects frequently it is important to do releases on a regular basis (and also to increment the version numbers then).
This ensures that all project relying on this are kept stable.

A release is done using the mvn release plugin (https://maven.apache.org/maven-release/maven-release-plugin/index.html).

https://pragmaticminds.atlassian.net/wiki/spaces/OR/pages/44204050/Release-Prozess

### Versioning

The Versions are build with the scheme `major.minor.bugfix` and the working version has always the appendix `-SNAPSHOT`.

### Release Rules

TODO Kerstin?

#### When to release?

#### Major / Minor / Bugfix?

#### Who releases?

### Bugfix Release

A bugfix / implementation release is pretty simple.
First, ensure that everything is commited and pushed!

You can do a dry run to check if your release will go through

```
mvn release:prepare -DdryRun=true
```

Then, prepare using
```
mvn clean release:prepare
```
The release gets the current version (without SNAPSHOT).
The new working version is the same with INCREMENTED BUGFIX and SNAPSHOT appended.
This should be the default in the prompts.

If there are any problems during performing the preparation one possibly has to clean the release using
```
mvn release:clean
```

Now, do the release by using
```
mvn release:perform
```

**After this do a push**

### Minor Release

Minor (as well as Major) releases are tracked in a seperate branch which keeps alive (e.g. for possible bugfixes).
To create a branch use

```
mvn release:branch -DbranchName={xxx}
```
Release Candidates are named `rc-{major}.{minor}`, e.g. `rc-0.1`.

The Plugin automatically increases the Version for the master to the version prompted.
Thus, in the prompt
* increase the minor version
* set the bugfix version to 0
Keep the SNAPSHOT.

The `rc` branch is created automatically and contains the "old" release number.
A release there is done as stated above with a bugfix release.

**After this do a push**

**Important: From the branch no Release artifact is generated and deployed to NEXUS. THUS a "bugfix" release has to be done "by hand" in the new BRANCH to ensure that a RELEASE Version is deployed to NEXUS**

### Major Release (???)