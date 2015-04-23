/** Small helper script to create indexes in the mongodb. */
var debug = require('debug')('mongoindex');
var Db = require('mongodb').Db;
var Server = require('mongodb').Server;

var server = process.env.MONGOHOST || 'localhost';
var serverport = parseInt(process.env.MONGOPORT) || 27017;
var dbname = process.env.MONGODB || 'test';
var dburl = 'mongodb://'+server+':'+serverport+'/'+dbname;
debug("mongodb: " + dburl);

// connect to the db
var db = new Db(dbname, 
		new Server(server, serverport, {auto_reconnect: true}), 
		{safe: true});

db.open(function(err, db) {
    if (err) {	
	debug('db open failed',err);
	process.exit(-1);
    }

    var handleres = function(err, result) {
	debug("ensure index: " + result);
	if (err) { 
	    debug("failed to ensure index: " + err);
	}
    };

    // ensure indexes for known collections
    var cols = [
	'fathomstats',
	'homenet',
	'baseline',
	'debugtool',
	'domainperf',
	'pageload'];

    _.each(cols,function(key) {
	var collection = db.collection(key);

	// per item index for quaranteed uniqueness
	collection.ensureIndex(
	    {uuid:1, objectid:1},
	    {unique: true}, 
	    handleres,
	);

	// per user index for quick access to single user data
	collection.ensureIndex(
	    {uuid:1},
	    {unique: false}, 
	    handleres
	);
    });
});
