# WIP

This is not near feature ready.

# MetaInflux

InfluxDB is used to store timeseries that later can be visualized into different tools. For example, you may want to use Grafana to visualize
your metrics. Let's say you are building your own UI and would like to add auto-complete for tags, metrics and tag values.

You can use the several [show](https://docs.influxdata.com/influxdb/v1.8/query_language/explore-schema/) influxql commands to do it. 
Which will work most of the time. Those command have several cool things
build into it, but let's say you type `xpu` and still would like the system to show the measurement `cpu` for you. You cannot easily do this.

So the idea: build another layer on top of InfluxDB (our MetaInflux as I'm calling it) which syncs with InfluxDB and allow you to use SQL 
to query that "DDL"type of data. Example, you want all the measurement fields with `edit_distance <= 1` for a certain token:

`SELECT name from fields where edit_distance(measurement) <= 1;`


## Tables

Right now we have two tables:

- `fields` with columns: `measurement`, `name`
- `tags` with columns: `measurement`, `name`, `value`


# TODO
- [X] Sync with InfluxDB every X period (needs proper test)
- [ ] Implement command line arguments using `Clap`
- [ ] Research how to update a "table" on DataFusion
- [ ] Implement Flight Server