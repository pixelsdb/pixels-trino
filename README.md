# pixels-trino
The Pixels connector for Trino.

**This project is under development, and not ready to be used.**

## Compatibility
Pixels integration (connector & event listener) is currently compatible with Trino 374. 
Other Trino versions that are compatible
with the Connector SPI in Trino 374 should also work well with Pixels.

## Build
This project can be opened as a maven project in Intellij and built using maven.

**Note** that Trino 374 requires Java 11, thus this project should be build by Jdk 11 (11.0.11 is tested).

[Pixels](https://github.com/pixelsdb/pixels) is the parent of this project,
therefore use `mvn install` to install Pixels modules into your local maven repository,
before building this project.

## Use Pixels in Trino

Ensure that Pixels and other prerequisites are installed following the instructions
[here](https://github.com/pixelsdb/pixels#installation-in-aws).