giant-squash
============

giant-squash is a data collector of hbase table sizes. 
This command-line tool does one thing: it collects data on hbase table sizes 
and, on shutdown, writes it out to a json file. The usual story is that this data 
is then fed to the [bloom-harvester](https://github.com/com.opower/bloom-harvester)
for maximum enjoyment.

To see a demo of how the data from this tool can be used, see
[The Story of the Big Data Elders](http://opower.github.io/2013/07/07/the-story-of-the-big-data-elders/)
and [The Big Data Elders, Archeology Hour](http://opower.github.io/link-tbd).

Requirements
------------
You must be able to run this jar from a gateway node or any machine on which you can
do ```hadoop fs -du -s /bla```.

See also
------------

* [little-rabbit](https://github.com/opower/little-rabbit): The companion tool of giant-squash. This
one collects job data from the job tracker.
* [bloom-harvester](https://github.com/opower/bloom-harvester): This is the tool that will eat your
squash and rabbits and produce a spectacle for your eyes. 

Quick start
-----------

* ``mvn clean install``
* ``java -Xmx3G -jar ./giant-squash-1.0-jar-with-dependencies.jar -output <giant-squashes.json> -interval <poll_interval_in_seconds> -tableNames <space delimited list of the table names>``
* ``Ctrl-C when done`` or ``kill -2 <pid`` if you run it with ``nohup`` in the background.
