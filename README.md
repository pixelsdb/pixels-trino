# pixels-trino
The Pixels connector for Trino.

## Compatibility
Pixels integration (connector & event listener) is currently compatible with Trino 405. 
Other Trino versions that are compatible
with the Connector SPI in Trino 405 should also work well with Pixels.

## Build
This project can be opened as a maven project in Intellij and built using maven.

**Note** that Trino 405 requires Java 17, thus this project should be build by Jdk 17 (17.0.6 is tested).

[Pixels](https://github.com/pixelsdb/pixels) is the parent of this project,
therefore use `mvn install` to install Pixels modules into your local maven repository,
before building this project.

## Use Pixels in Trino

Follow the instructions
[HERE](https://github.com/pixelsdb/pixels/blob/master/docs/INSTALL.md) to install Pixels and use it in Trino.