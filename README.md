# pixels-trino
The Pixels connector for Trino.

## Compatibility
Pixels integration (connector & event listener) is currently compatible with Trino 465. 
Other Trino versions that are compatible
with the Connector SPI in Trino 465 should also work well with Pixels.

## Build
This project can be opened as a maven project in Intellij and built using maven.

**Note** that Trino 465 requires Java 23.0.1+, thus this project should be built by JDK 23 (Azul Zulu JDK 23.0.1+11 is tested).

[Pixels](https://github.com/pixelsdb/pixels) is the parent of this project,
therefore use `mvn install` to install Pixels modules into your local maven repository,
before building this project.

## Use Pixels in Trino
Follow the instructions in
[Pixels Installation](https://github.com/pixelsdb/pixels/blob/master/docs/INSTALL.md) to install Pixels and other components except Trino.
After that, follow this document to install Trino and use it to query Pixels.
Ensure Java 23 is in use as it is required by Trino 465.

### Install Trino

Download and install Trino 465 following the instructions in [Trino Docs](https://trino.io/docs/465/installation/deployment.html).

Here, we install Trino to `~/opt/trino-server-465` and create a link for it:
```bash
cd ~/opt; ln -s trino-server-465 trino-server
```
Then download [trino-cli](https://trinodb.github.io/docs.trino.io/465/client/cli.html) into `~/opt/trino-server/bin/`
and give the executable permission to it.
Some scripts in Trino may require python:
```bash
sudo apt-get install python
```


### Install Pixels Connector
There are two important directories in the home of trino-server: `etc` and `plugin`.
To install Pixels connector, decompress `pixels-trino-connector-*.zip` into the `plugin` directory.
The `etc` directory contains the configuration files of Trino.
In addition to the configurations mentioned in the official docs, 
create the catalog config file named `pixels.properties` for Pixels in the `etc/catalog` directory, with the following content:
```properties
connector.name=pixels

# serverless config
# it can be on, off, auto, or session
cloud.function.switch=off
clean.intermediate.result=true
```
**Note** that `etc/catalog/pixels.proterties` under Trino's home is different from `PIXELS_HOME/pixels.properties`.
In Trino, Pixels can push projections, filters, joins, and aggregations into serverless computing services (e.g., AWS Lambda).
This feature is named `Pixels-Turbo` and can be turned on by setting `cloud.function.switch` to `auto` (adaptively enabled) or `on` (always enabled).
Turn it `off` to only use Trino workers for query processing.
We can also set it to `session` so that this switch can be dynamically turned on or off by the session properties `pixels.cloud_function_enabled`.
This allows `pixels-server` choosing whether to execute the query with cloud functions enabled.

Append the following two lines into `etc/jvm.config`:
```config
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
```
Thus, pixels can reflect internal or low-level classes to improve performance. This is only needed for Java 9+.

### Install Pixels Event Listener*
Pixels event listener is optional. It is used to collect the query completion information for performance evaluations.
To install the event listener, decompress `pixels-trino-listener-*.zip` into the `plugin` directory.

Create the listener config file named `event-listener.properties` in the `etc` directory, with the following content:
```properties
event-listener.name=pixels-event-listener
enabled=true
listened.user.prefix=none
listened.schema=pixels
listened.query.type=SELECT
log.dir=/home/ubuntu/opt/pixels/listener/
```
`log-dir` should point to
an existing directory where the listener logs will appear.


### Run Queries

Start Pixels + Trino following the instructions in [Starting Pixels + Trino](https://github.com/pixelsdb/pixels/blob/master/docs/INSTALL.md).
Then, you can test the query performance following [TPC-H Evaluation](https://github.com/pixelsdb/pixels/blob/master/docs/TPC-H.md).
