# vision-zero-analtyics

> Vision Zero Analytics

## Build Setup

``` bash
# install dependencies
npm install

# serve with hot reload at localhost:8080
npm run dev

# build for production with minification
npm run build
```

For detailed explanation on how things work, consult the [docs for vue-loader](http://vuejs.github.io/vue-loader).



--- extra info

https://wikis.nyu.edu/display/NYUHPC/Sharing+files+on+the+NYU+HPC+clusters

Hue: http://babar.es.its.nyu.edu:8888/home

Hive: jdbc:hive2://babar.es.its.nyu.edu:10000/




<!-- select precinct, year, month, amount, avg(amount) over (
        PARTITION BY precinct
        ORDER BY year, month
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
FROM violations
where precinct = 70
and year = 2016; -->