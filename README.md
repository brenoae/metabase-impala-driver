# Metabase Impala Driver

The Metabase Impala driver allows Metabase v0.32.0 or above to connect to Impala databases.
This driver uses Impala's JDBC driver but due to license restrictions it's not possible to include the JDBC here. But this driver can be easily downlowded following instructions below.

## Downloading and Installing Impala Driver

### Downloading the Impala JDBC Driver

You can download the JDBC driver from [Impala's JDBC driver downloads page](http://www.cloudera.com/downloads/connectors/impala/jdbc.html).
The downloaded archive files contains ImpalaJDBC41_[Version].zip

### Downloading Impala Metabase Driver

[Click here](https://github.com/brenoae/metabase-impala-driver/releases/latest) to view the latest release of the Metabase Impala driver; click the link to download `impala.metabase-driver.jar`.

### How to Install it

Metabase will automatically make the Impala driver if it finds the driver JAR in the Metabase plugins directory when it starts up.

Follow steps shown bellow to install the driver properly:
1. Create the directory (if it's not already there)
2. Move the Impala metabase driver JAR you just downloaded into it ("impala.metabase-driver.jar")
3. Move the Impala JDBC driver JAR you just downloaded into it ("ImpalaJDBC41.jar")
4. Restart Metabase

By default, the plugins directory is called `plugins`, and lives in the same directory as the Metabase JAR.

For example, if you're running Metabase from a directory called `/app/`, you should move the Impala driver JAR to `/app/plugins/`:

```bash
# example directory structure for running Metabase with Impala support
./metabase.jar
./plugins/impala.metabase-driver.jar
./plugins/ImpalaJDBC41.jar
```

If you're running Metabase from the Mac App, the plugins directory defaults to `~/Library/Application Support/Metabase/Plugins/`:

```bash
# example directory structure for running Metabase Mac App with Impala support
/Users/camsaul/Library/Application Support/Metabase/Plugins/impala.metabase-driver.jar
/Users/camsaul/Library/Application Support/Metabase/Plugins/ImpalaJDBC41.jar
```

If you are running the Docker image or you want to use another directory for plugins, you should specify a custom plugins directory by setting the environment variable `MB_PLUGINS_DIR`.


## Building the Impala Driver Yourself

### Prereq: Install Metabase as a local maven dependency, compiled for building drivers

Clone the [Metabase repo](https://github.com/metabase/metabase) first if you haven't already done so.

```bash
cd /path/to/metabase/
lein install-for-building-drivers
```

### Build it

```bash
cd /path/to/metabase-impala-driver
lein clean
DEBUG=1 LEIN_SNAPSHOTS_IN_RELEASE=true lein uberjar
```

This will build a file called `target/uberjar/impala.metabase-driver.jar`; copy this to your Metabase `./plugins` directory.