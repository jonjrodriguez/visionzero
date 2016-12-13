# Server
Local python 2.7 web server that gives api access to Impala hosted on Dumbo.

## Install Dependencies
`pip install <dependency>`

* [flask](http://flask.pocoo.org/)
* [simplejson](https://simplejson.readthedocs.io/en/latest/)
* [impyla](https://github.com/cloudera/impyla)
* [sshtunnel](https://github.com/pahaz/sshtunnel)

## Usage
1. Copy config.py.sample to config.py:
  * Update fields
2. `python run.py`
3. Navigate to http://127.0.0.1:5000/ in browser


### Client
Javascript single page application using [Vue.js](http://vuejs.org/)