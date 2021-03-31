# Hadoop Extension

This module extends Schema Registry and allows storing files on HadoopFS. It should be noted that we also use
HDFS as a proxy for storing files on S3/ABFS.

We also use Hadoop for verifying Kerberos permissions, so that is an additional functionality which is supported
through this plugin.

Hadoop comes with a lot of transitive dependencies which impact Schema Registry. We don't want to have
these dependencies on our classpath. For this reason we use a ClasspathLoader, which loads the Hadoop
classes only when necessary.

The diagram below shows the distribution of classes.

`HadoopPluginImpl` is the only class which actually works with Hadoop classes. For this reason it can
only be accessed when we have loaded Hadoop's classpath.

The remaining classes (`HdfsFileStorage`, `HadoopPlugin`, `HadoopPluginClassLoader`, etc.) are used mainly for
the classloading mechanism and also for backward compatibility with earlier versions of Schema Registry. Please
be careful and do not move `HdfsFileStorage` to another package, or change any of its public methods, because it
can have unforeseen consequences on our external clients.

![Flow chart](flowchart.png)

## Configuration

FileStorage is configured through `registry.yaml`. When Schema Registry starts, the name of the
class is read by `FileStorageProvider` and creates an instance. This file storage *does not* have
to be HDFS, it can be any other implementation. In order to remain compatible, we need to have
HdfsFileStorage implement the interface `FileStorage` and it needs to have a public construction
which accepts a `FileStorageConfiguration` object.

# Artifacts

This module produces two JAR files:

* `-shim` containing classes which can be visible from the main classpath
* *regular* JAR file which contains the classes only visible on the Hadoop classpath

We also put all the dependent JAR files under `build/dependency`

You should check the project file of `registry-dist` to see how these JAR files are moved around to
construct a working directory structure for the extension.