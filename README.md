# dslab.playandgo.analytics

This project allow to import data from the Play&Go dbs and create a dataset for mobility analytics. 
Also a simplified version of somo data are exported in order to provide some API.

## Import mobility data for analysis
Starting from the existing data, the component performe the following stesp.

#### Map tracks to Street Graph
Using Valhalla routing engine the tracks are mapped to the OSM graph elements

#### Map points to H3 elements
Points are mapped also to an hexagon using Huber H3 library

#### Map points to graph node
If the track is contained in a specific area defined for territory, points are also mappe to OSM node_id using OSMnx library

#### Additional data
Other informations about track are retrieved (player, campaign, mode, time, distance, duration, group, etc…)

#### Storing Data
The resulting data are stored in Parquet files, divided by territory and years

#### Reference
- OSMnx 2.1.0 [url](https://osmnx.readthedocs.io/en/stable/)
- How to get a custom OSM file [url](https://pyrosm.readthedocs.io/en/latest/basics.html#what-to-do-if-you-cannot-find-the-data-for-your-area-of-interest)

## Define simple data analysis API

#### Convert track data
Starting from Parquet files, tracks data are mapped to a lower resolution H3 hexagon. Some specific relevant information are added (starting H3 hexagon, ending H3 exagon, time slot, etc…)

#### Group by multimodal trip
The tracks data are also grouped by multimodal trip. A multimodal trip is a trip composed by multiple tracks traveled with different means

#### Store data by campaign
Data are store by campaign, using the DuckDB file format.

#### Define analytics logic
Analytics logics are provided, querying the DuckDB file for filtering data, and performing other aggregation functions. For example the following API are provided:
- trip average duration starting or arriving from/to a specific hexagon
- total number of tracks passing through every hexagon
- number of unique user starting or arriving from/to a specific hexagon

## Scheduler 
This component should be invocked periodically in order to import new data from the Play&Go dbs and update the Parquet files, and also updating the DuckDb files.
```sh
python scheduler.py --period 2592000 --territories TAA TEST
```
The following parameters are allowed:
- `--period` : a mandatory parameter; it define the period in seconds that will be added for the next scheduled run.
- `--territories` : an optional list of territory ids; if present only that territories will be updated, otherwise all the territories are considered. 

The component can read (o write after the first execution) a json file `territories_time_ranges.json` that can specify, for every territory, a specific period for extract new data.
The json file has the following format:
```json
[
  {
    "territory_id": "TAA",
    "start_time": "2026-01-01T00:00:00+00:00",
    "end_time": "2026-12-31T23:59:59+00:00"
  }
]
```
List of env variables used by the component:
- `STORAGE_PATH` : the directory where the component store all the files and the configuration file `territories_time_ranges.json`
- `PG_MONGO_URI` : Play&Go Mongo connection uri
- `PG_MONGO_DB` : Play&Go db name
- `PG_MONGO_DIRECT_CONNECTION` : True for local connection, otherwise False (default False)
- `PG_COMPANY_MONGO_URI` : Play&Go Company Mongo connection uri
- `PG_COMPANY_MONGO_DB` : Play&Go Company db name
- `PG_COMPANY_MONGO_DIRECT_CONNECTION` : True for local connection, otherwise False (default False)
- `PG_HSC_MONGO_URI` : Play&Go HSC Mongo connection uri
- `PG_HSC_MONGO_DB` : Play&Go HSC db name
- `PG_HSC_MONGO_DIRECT_CONNECTION` : True for local connection, otherwise False (default False)
- `VALHALLA_URI` : Valhalla server uri 
- `START_TIME` : default start time if no entry for that territory is present in conf file, default `2026-05-01T00:00:00+00:00`
- `END_TIME` : default start time if no entry for that territory is present in conf file, default `2026-05-31T23:59:59+00:00`

## Analytics API
This component expose some REST API 
```sh
python analytics-api.py
```
List of env variables used by the component:
- `STORAGE_PATH` : the directory where the component store all the files and the configuration file `territories_time_ranges.json`
- `SERVER_PORT` : listening port
