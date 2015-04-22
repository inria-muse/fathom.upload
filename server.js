var debug = require('debug')('uploader')
var _ = require('underscore');
var express = require('express');
var multiparty = require('multiparty');
var bodyParser = require('body-parser')
var redis = require("redis");
var moment = require("moment");
var Db = require('mongodb').Db;
var Server = require('mongodb').Server;

// configs
var redisdb = parseInt(process.env.REDISDB) || 3;
var server = process.env.MONGOHOST || 'localhost';
var serverport = parseInt(process.env.MONGOPORT) || 27017;
var dbname = process.env.MONGODB || 'test';
var port = parseInt(process.env.PORT) || 3000;

var dburl = 'mongodb://'+server+':'+serverport+'/'+dbname;
debug("mongodb: " + dburl);

// redis cli for runtime stats
var client = redis.createClient();
client.select(redisdb, function(res) {
    debug("Redis select " + res);
});

client.on("error", function(err) {
    debug("Redis error " + err);
});

// connect to the db
var db = new Db(dbname, 
		new Server(server, serverport, {auto_reconnect: true}), 
		{safe: true});

db.open(function(err, db) {
    if (err) {
	console.error(err);
	process.exit(-1);
    }

    // ensure indexes for known collections
    _.each(['fathomstats','homenet','baseline','debugtool'],function(key) {
	var collection = db.collection(key);

	// per item index for quaranteed uniqueness
	collection.ensureIndex(
	    {uuid:1, objectid:1},
	    {unique: true}, 
	    function(err, result) {
		if (err) { 
		    debug("failed to set index: " + err);
		    process.exit(-1);
		}
	    }
	);

	// per user index for quick access to single user data
	collection.ensureIndex(
	    {uuid:1},
	    {unique: false}, 
	    function(err, result) {
		if (err) { 
		    debug("failed to set index: " + err);
		    process.exit(-1);
		}
	    }
	);
    });
});

// reset stats
var rstats = 'fathomupload';
client.hmset(rstats, {
    start : new Date(),
    removeusercnt : 0,
    uploadcnt : 0,
    lastupload : 0,
    errorcnt : 0,
    lasterror : 0 });

// main app
var app = express();

// we'll be behind a proxy
app.enable('trust proxy');

// middleware

// parse application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({limit: '100mb', extended: false }))

// parse application/json
app.use(bodyParser.json({limit: '100mb'}));

// default error handler
app.use(function(err, req, res, next){
    debug(err);
    debug(err.stack);

    try {
	if (client) {
	    client.hmset(rstats, { lasterror : new Date() });
	    client.hincrby(rstats, "errorcnt", 1);
	}
    } catch(e) {
    }

    res.type('application/json');
    res.status(500).send({ error: "internal server error",
			   details: err});
});

// GET returns some basic stats about the server
app.get('/*', function(req, res){
    client.hgetall(rstats, function(err, obj) {
	res.type('text/plain');
	obj.uptime = "Started " + moment(new Date(obj.start).getTime()).fromNow();
	res.status(200).send(JSON.stringify(obj,null,4));
    });
});

// Handle requests to remove user data
app.all('/removeuser/:uuid', function(req,res) {
    var obj = {
	uuid : req.params.uuid, 
	ts : new Date().getTime(),
	req_ip : req.ip
    };

    var collection = db.collection("removeuserreqs");
    collection.insert(obj, function(err, result) {
	if (err) {
	    debug("failed to save removeuserreq to mongodb: " + err);

	    res.type('application/json');
	    res.status(500).send({error: "internal server error",
				  details: err});
	} else {
	    client.hincrby(rstats, "removeusercnt", c);
	    res.status(200).send();
	    // TODO: send email to notify muse.fathom@inria.fr ?
	}
    });
});

// POST handles uploads
app.post('/*', function(req,res) {
    var c = 0;
    var docs = {};

    var escapestr = function(s) {
        return s.replace(/\./g,'__dot__').replace(/\$/g,'__dollar__');
    }
    
    var escape = function(obj) {
	if (_.isArray(obj))
	    return _.map(obj, escape);

	_.each(obj, function(value,oldkey) {
	    var newkey = escapestr(oldkey);
	    if (newkey !== oldkey)
		debug("escape: " + oldkey + " -> " + newkey);
            if (_.isObject(value) || _.isArray(value)) {
                obj[newkey] = escape(value);
            } else {
		// primitive value
                obj[newkey] = value;
            }
	    delete obj[oldkey];
        });
        return obj;
    }
    
    var adddoc = function(obj) {
	// required fields
	if (!obj.collection || !obj.uuid || !obj.objectid) {
	    debug("invalid object: " + JSON.stringify(obj));  
	    return;
	}

	var collectionname = obj.collection;
	delete obj.collection;

	// convert few known date fields to native mongo format
	if (obj['ts'])
	    obj['ts'] = new Date(obj['ts']);
	if (obj['uploadts'])
	    obj['uploadts'] = new Date(obj['uploadts']);
	if (obj['queuets'])
	    obj['queuets'] = new Date(obj['queuets']);

	// add some more metadata to the object
	obj.upload = { 
	    serverts : new Date(),
	    req_ip : req.ip 
	};

	if (!docs[collectionname])
	    docs[collectionname] = [];
	docs[collectionname].push(obj);
	c += 1;
    };

    var savedocs = function() {
	if (c === 0) {
	    client.hmset(rstats, { lasterror : new Date() });
	    client.hincrby(rstats, "errorcnt", 1);

	    res.type('application/json');
	    res.status(500).send({error: "got zero objects"});
	    return;
	}

	debug("saving " + c + " items");  

	var error = undefined;
	_.each(docs, function(value, key) {
	    if (error) return; // stop on first error

	    // insert batch to the collection
	    debug("save " + value.length + " items to " + key);

	    var collection = db.collection(key);
	    collection.insert(value, function(err, result) {
		if (err) {
		    debug('save err ' + err);
                    if (("" + err).indexOf('duplicate key error')>=0) {
			// ignore: something was uploaded twice
		    } else if (("" + err).indexOf('must not contain')>=0) {
                        // HACK --
                        // MongoDB hack needed, no dots or dollar signs 
			// allowed in key names ..
			debug("mongo escape hack");
                        value = escape(value);
			// try again
	                collection.insert(value, function(err, result) {
			    debug('save err after hack ' + err);
			    if (err && 
				("" + err).indexOf('duplicate key error')>=0) 
			    {
				// ignore: something was uploaded twice
			    } else if (err) {
		                error = err;
                            }
                        });
                        // HACK end --
                    } else {
		        error = err;
                    }
		}
	    });
	}); // each
	    
	if (error) {
	    debug('saving failed: ' + JSON.stringify(error));
	    client.hmset(rstats, { lasterror : new Date() });
	    client.hincrby(rstats, "errorcnt", 1);

	    res.type('application/json');
	    res.status(500).send({error: "internal server error",
				  details: error});
	} else {
	    // stats
	    client.hmset(rstats, { lastupload : new Date() });
	    client.hincrby(rstats, "uploadcnt", c);
	    res.sendStatus(200);
	}
    };
    
    debug("upload from " + req.ip + " as " + req.get('Content-Type') + 
	 " size " + req.get('Content-Length'));

    if (req.get('Content-Type').indexOf('multipart/form-data')>=0) {
	var form = new multiparty.Form({maxFields : 10000});

	form.on('part', function(part) {
	    // handle new part (json object)
	    var json = '';
	    part.on('data', function(chunk) {
		json += chunk;
	    });
	    part.on('end', function() {
		var obj = JSON.parse(json);
		if (obj && obj.collection) {
		    adddoc(obj);
		} else {
		    debug("invalid data: " + json);
		}
	    });
	});
	
	form.on('error', function(err) {
	    debug(err);
	    debug(err.stack);

	    client.hmset(rstats, { lasterror : new Date() });
	    client.hincrby(rstats, "errorcnt", 1);

	    res.type('application/json');	    
	    res.status(500).send({ error: "internal server error",
				   details: err});
	});
	
	form.on('close', function(err) {
	    savedocs();
	});

	// start parsing the stream
	form.parse(req);

    } else if (req.get('Content-Type').indexOf('application/x-www-form-urlencoded')>=0 || req.get('Content-Type').indexOf('json')>=0) 
    {
	var objs = req.body;
	if (objs && _.isArray(objs)) {
	    _.each(objs, function(obj) {
		adddoc(obj);
	    });
	    savedocs();
	} else if (objs && _.isObject(objs) && objs.collection) {
	    adddoc(objs);
	    savedocs();
	} else {
	    debug("invalid req.body: " + JSON.stringify(req.body));

	    client.hmset(rstats, { lasterror : new Date() });
	    client.hincrby(rstats, "errorcnt", 1);

	    res.type('application/json'); 
	    res.status(500).send({error: "invalid data"});
	}
    } else {
	client.hmset(rstats, { lasterror : new Date() });
	client.hincrby(rstats, "errorcnt", 1);

	res.type('application/json');
	res.status(500).send({error: "unhandled content type: "+req.get('Content-Type')});
    }
});

// start!
var server = app.listen(port, function() {
    debug("listening on %s:%d",
	  server.address().address, server.address().port);
});
