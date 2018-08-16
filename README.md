# news-trends-broadcast

The project is about listening for RSS feeds:

- news from CNN,
- trends from Google.

News get filtered by trends and saved to the database. Those news are made available for the outside world through the RESTful API (implemented in another project).

## Prerequisites

You need to have ```sbt``` and ```docker-compose``` installed on your machine.

## How to run

First go to the ```news-trends-broadcast``` project (this one) and issue the following command:

```
sbt assembly
```

It will create the assembly jar. After that start containers using docker-compose:

```
docker-compose up
```

The command tells docker-compose to start containers listed in ```docker-compose.yml``` file. Those are: rss listener and mongodb database.

Next go to the ```news-trends-rest-api``` project's root directory and do exactly the same:

```
sbt assembly
docker-compose up
```
It will start the container for the RESTful API.
