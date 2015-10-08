/*
   Fathom data upload server
   
   Copyright (C) 2015 Inria Paris-Roquencourt 
      
   The MIT License (MIT)
         
   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:
                              
   The above copyright notice and this permission notice shall be included in 
   all copies or substantial portions of the Software.
                                       
   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

/**
 * @fileoverfiew Fathom data upload server.
 * @author Anna-Kaisa Pietilainen <anna-kaisa.pietilainen@inria.fr> 
 */                                                            
var debug = require('debug')('fathomupload')
var _ = require('underscore');
var express = require('express');
var multiparty = require('multiparty');
var bodyParser = require('body-parser')
var redis = require("redis");
var moment = require("moment");
var MongoClient = require('mongodb').MongoClient;

// configs
const REDISDB = (process.env.REDISDB ? parseInt(process.env.REDISDB) : 3);
const PORT = (process.env.PORT ? parseInt(process.env.PORT) : 3003);
const MONGOHOST = process.env.MONGOHOST || 'localhost';
const MONGOPORT = (process.env.MONGOPORT ? parseInt(process.env.MONGOPORT) : 27017);
const MONGODB = process.env.MONGODB || 'test';
const MONGOURL = 'mongodb://'+MONGOHOST+':'+MONGOPORT+'/'+MONGODB;

debug("mongodb: " + MONGOURL);

// catch all 
process.on('uncaughtException', function(e) {
    debug('unhandled exception: ' +(e instanceof Error ? e.message : e));
    throw e;
});

// connect to the db
var db = undefined;
MongoClient.connect(MONGOURL, {
    db : { w : 1, native_parser : true},
    server : { poolSize : 10}
}, function(err, _db) {
    if (err) {	
    	debug('db open failed',err);
	   process.exit(-1);
    } else {
	   db = _db;
    }
});

// redis cli for runtime stats
var client = redis.createClient();
client.select(REDISDB, function(err) {
    if (err)
	debug("redis select error: " + err);
});
client.on("error", function(err) {
    if (err)
	debug("redis error: " + err);
});

// reset stats
var rstats = 'fathomupload';
client.hmset(rstats, {
    start : new Date(),
    removeusercnt : 0,
    uploadcnt : 0,
    lastupload : 0,
    errorcnt : 0,
    lasterror : 0 }
);

var incerror = function() {
    try {
    	if (client) {
    	    client.hmset(rstats, { lasterror : new Date() });
    	    client.hincrby(rstats, "errorcnt", 1);
    	}
    } catch(e) {
    }
};

var incupload = function(c) {
    try {
    	if (client) {
    	    // stats
    	    client.hmset(rstats, { lastupload : new Date() });
    	    client.hincrby(rstats, "uploadcnt", c);
    	}
    } catch(e) {
    }
};

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
    incerror();
    res.type('application/json');
    res.status(500).send({ error: "internal server error",
			   details: err});
});

// some basic stats about the server
app.get('/status', function(req, res){
    client.hgetall(rstats, function(err, obj) {
	res.type('text/plain');
	obj.uptime = "Started " + moment(new Date(obj.start).getTime()).fromNow();
	obj.desc = "Fathom upload server";
	obj.copy = "Copyright 2014-2015 MUSE Inria Paris-Rocquencourt";
	obj.contact = "muse.fathom@inria.fr";
	res.status(200).send(JSON.stringify(obj,null,4));
    });
});

// POST handles uploads
app.post('/*', function(req,res) {
    var c = 0;
    var docs = {};

    var escapestr = function(s) {
        return (s ? s.replace(/\./g,'__dot__').replace(/\$/g,'__dollar__') : s);
    };
    
    var escape = function(obj) {
	if (_.isArray(obj)) {
	    obj = _.map(obj, escape);
	} else if (_.isObject(obj)) {
	    _.each(obj, function(value,key) {
    		var newkey = escapestr(key);
    		if (newkey !== key) {
    		    debug("escape: " + key + " -> " + newkey);
    		    delete obj[key];
    		    key = newkey;
    		}
            obj[key] = escape(value);
        });
	}
        return obj;
    };
    
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
    	    debug('got zero objects');
    	    incerror();
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
    	    collection.insertMany(value, function(err, result) {
    		if (err) {
    		    debug('insertMany failed: ' + JSON.stringify(error));

                        if (("" + err).indexOf('duplicate key error')>=0) {
    			// ignore: something was uploaded twice
    			// can happen on errors
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
    			// some other error
    		        error = err;
                        }
    		} else {
    		    debug('inserted ' + result.result.n + ' documents');
    		}
    	    });
    	}); // each
    	    
    	if (error) {
    	    incerror();
    	    res.type('application/json');
    	    res.status(500).send({error: "internal server error",
    				  details: error});
    	} else {
    	    incupload(c);
    	    res.status(200).send();
    	}
    }; // savedocs
    
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
	    incerror();
	    res.type('application/json');	    
	    res.status(500).send({ error: "internal server error",
				   details: err});
	});
	
	form.on('close', function(err) {
	    savedocs();
	});

	// start parsing the stream
	form.parse(req);

    } else if (req.get('Content-Type').indexOf('application/x-www-form-urlencoded')>=0 || req.get('Content-Type').indexOf('json')>=0) {
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
    	    incerror();
    	    res.type('application/json'); 
    	    res.status(500).send({error: "invalid data"});
    	}
    } else {
    	debug("unknown content-type: " + +req.get('Content-Type'));
    	incerror();
    	res.type('application/json');
    	res.status(500).send({
    	    error: "unhandled content type: "+req.get('Content-Type')
    	});
    }
}); // post route

// catch everything else
app.use(function(req, res, next) {
    res.status(404).send('Sorry cant find that!');
});

// start!
var server = app.listen(PORT, function() {
    debug("listening on %s:%d",
	  server.address().address, server.address().port);
});