# pixels-trino
The Pixels connector for Trino.

## Compatibility
Pixels integration (connector & event listener) is currently compatible with Trino 375. 
Other Trino versions that are compatible
with the Connector SPI in Trino 375 should also work well with Pixels.

## Build
This project can be opened as a maven project in Intellij and built using maven.

**Note** that Trino 375 requires Java 11, thus this project should be build by Jdk 11 (11.0.11 is tested).

[Pixels](https://github.com/pixelsdb/pixels) is the parent of this project,
therefore use `mvn install` to install Pixels modules into your local maven repository,
before building this project.

## Use Pixels in Trino

Follow the instructions
[here](https://github.com/pixelsdb/pixels#installation-in-aws) to install Pixels and use it in Trino.