var debug = require('debug')('uploader')
var _ = require('underscore');
var argv = require('minimist')(process.argv.slice(2));
var express = require('express');
var multiparty = require('multiparty');
var bodyParser = require('body-parser')
var redis = require("redis");
var moment = require("moment");

// mongo stuff
var Db = require('mongodb').Db;
var Server = require('mongodb').Server;

debug(JSON.stringify(argv));
if (argv.h || argv._.length !== 1) {
  console.log("Usage: " + process.argv[0] + 
	" " + process.argv[1] + 
	" [-p <port>] [-s server] [-q server_port] db");
  process.exit(0);
}

// redis cli for runtime stats
var client = redis.createClient();
client.select(2, function(res) {
    debug("Redis select " + res);
});
client.on("error", function(err) {
    debug("Redis error " + err);
});

var port = argv.p || 3001;
var server = argv.s || 'localhost';
var serverport = argv.q || 27017;

var dbname = argv._[0];
var dburl = 'mongodb://'+server+':'+serverport+'/'+dbname;
debug("mongodb: " + dburl);

// connect to the db
var db = new Db(dbname, 
		new Server(server, serverport, {auto_reconnect: true}), 
		{safe: true});
db.open(function(err, db) {
    if (err) {
	console.error(err);
	process.exit(-1);
    }
});

// reset stats
client.hmset("stats", {
    start : new Date(),
    uploadcnt : 0,
    lastupload : 0,
    lasterror : 0 });

// main app
var app = express();

// we'll be behind a proxy
app.enable('trust proxy');

// middleware

// parse application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: false }))

// parse application/json
app.use(bodyParser.json())

// default error handler
app.use(function(err, req, res, next){
    debug(err);
    debug(err.stack);
    res.type('application/json');
    res.status(500).send({ error: "internal server error",
			   details: err});
});

// GET returns some basic stats about the server
app.get('/*', function(req, res){
    client.hgetall("stats", function(err, obj) {
	res.type('application/json');
	obj.uptime = "Started " + moment(new Date(obj.start).getTime()).fromNow();
	res.status(200).send(obj);
    });
});

// POST handles uploads
app.post('/*', function(req,res) {
    var c = 0;
    var docs = {};
    var adddoc = function(obj) {
	// convert few known date fields to native mongo format
	if (obj['ts'])
	    obj['ts'] = new Date(obj['ts']);
	if (obj['uploadts'])
	    obj['uploadts'] = new Date(obj['uploadts']);
	if (obj['queuets'])
	    obj['queuets'] = new Date(obj['queuets']);

	// add some more metadata to the object
	obj.upload = { serverts : new Date(),
		       req_ip : req.ip };

	if (!docs[obj.collection]) {
	    docs[obj.collection] = [];
	}

	docs[obj.collection].push(obj);
	c += 1;
    };
    var savedocs = function() {
	if (c === 0) return;

	debug("saving " + c + " items");  
	var error = undefined;
	_.each(docs, function(value, key) {
	    if (error) return;	

	    // insert batch to the collection
	    debug("save " + value.length + " items to " + key);

	    var collection = db.collection(key);
	    collection.insert(value, {w:1}, function(err, result) {
		if (err)  error = err;
	    });
	}); // each
	    
	if (error) {
	    debug("failed to save data to mongodb: " + error);
	    client.hmset("stats", { lasterror : new Date() });

	    res.type('application/json');
	    res.status(500).send({error: "internal server error",
				  details: error});
	} else {
	    // stats
	    client.hmset("stats", { lastupload : new Date() });
	    client.hincrby("stats", "uploadcnt", c);
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
	    res.type('application/json'); 
	    res.status(500).send({error: "invalid data"});
	}
    } else {
	res.type('application/json');
	res.status(500).send({error: "unhandled content type: "+req.get('Content-Type')});
    }
});

// start!
var server = app.listen(port, function() {
    debug("listening on %s:%d",
	  server.address().address, server.address().port);
});
