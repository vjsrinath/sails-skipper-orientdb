/**
 * Created by developer on 5/18/15.
 */
/**
 * Module dependencies
 */

var path = require('path');
var util = require('util');
var Writable = require('stream').Writable;
var _ = require('lodash');
var concat = require('concat-stream');
var async = require('async');


/**
 * skipper-gridfs
 *
 * @param  {Object} globalOpts
 * @return {Object}
 */

module.exports = function OrientDBStore(globalOpts) {
    globalOpts = globalOpts || {};

    _.defaults(globalOpts, {
        maxChunkSize: 1000000, //1MB
        filesCollection: 'files',
        filesChunksCollection: 'files_chunks'
    });
    //var getConnection = _connectionBuilder(globalOpts);

    var files = function () {
            return globals[globalOpts.filesCollection];
        },
        filesChunks = function () {
            return globals[globalOpts.filesChunksCollection];
        };

    var adapter = {
        ls: function (dirname, cb) {

            files().find({where: {'metadata.dirname': dirname}}).then(function (err, files) {
                if (err) return cb(err);
                return cb(null, files);
            });

        },

        read: function (fd, cb) {
            console.log("Not implemented yet!!!");
            return;
            GridStore.exist(db, fd, globalOpts.bucket, function (err, exists) {
                if (err) {
                    db.close();
                    return cb(err);
                }
                if (!exists) {
                    err = new Error('ENOENT');
                    err.name = 'Error (ENOENT)';
                    err.code = 'ENOENT';
                    err.status = 404;
                    err.message = util.format('No file exists in this mongo gridfs bucket with that file descriptor (%s)', fd);
                    db.close();
                    return cb(err);
                }

                var gridStore = new GridStore(db, fd, 'r', {root: globalOpts.bucket});
                gridStore.open(function (err, gridStore) {
                    if (err) {
                        db.close();
                        return cb(err);
                    }
                    var stream = gridStore.stream();
                    stream.pipe(concat(function (data) {
                        db.close();
                        return cb(null, data);
                    }));

                    stream.on('error', function (err) {
                        db.close();
                        return cb(err);
                    });

                    stream.on('close', function () {
                        db.close();
                    });
                });
            });

        },

        readLastVersion: function (fd, cb) {
            console.log("Not implemented yet!!!");
            return;
            this.readVersion(fd, -1, cb);

        },

        readVersion: function (fd, version, cb) {
            console.log("Not implemented yet!!!");
            return;
            MongoClient.connect(globalOpts.uri, {native_parser: true}, function (err, db) {
                if (err) {
                    return cb(err);
                }
                var gfs = Grid(db, mongo);
                gfs.collection(globalOpts.bucket).ensureIndex({filename: 1, uploadDate: -1}, function (err, indexName) {
                    if (err) {
                        db.close();
                        return cb(err);
                    }

                    var cursor = gfs.collection(globalOpts.bucket).find({filename: fd});
                    if (version < 0) {
                        var skip = Math.abs(version) - 1;
                        cursor.limit(-1).skip(skip).sort({uploadDate: -1}); //'desc
                    } else {
                        cursor.limit(-1).skip(version).sort({uploadDate: 1}); //'asc'
                    }

                    cursor.next(function (err, file) {

                        if (err) {
                            console.log(err);
                            db.close();
                            return cb(err);
                        }
                        if (!file) {
                            err = new Error('ENOENT');
                            err.name = 'Error (ENOENT)';
                            err.code = 'ENOENT';
                            err.status = 404;
                            err.message = util.format('No file exists in this mongo gridfs bucket with that file descriptor (%s)', fd);
                            db.close();
                            return cb(err);
                        }

                        var gridStore = new GridStore(db, file._id, 'r', {root: globalOpts.bucket});
                        gridStore.open(function (err, gridStore) {
                            if (err) {
                                db.close();
                                return cb(err);
                            }

                            var stream = gridStore.stream();
                            stream.pipe(concat(function (data) {
                                db.close();
                                return cb(null, data);
                            }));

                            stream.on('error', function (err) {
                                db.close();
                                return cb(err);
                            });

                            stream.on('close', function () {
                                db.close();
                            });
                        });
                    });
                });
            });
        },

        rm: function (fd, cb) {
            console.log("Not implemented yet!!!");
        },

        /**
         * A simple receiver for Skipper that writes Upstreams to
         * gridfs
         *
         *
         * @param  {Object} options
         * @return {Stream.Writable}
         */
        receive: function GridFSReceiver(options) {
            options = options || {};
            options = _.defaults(options, globalOpts);

            var receiver__ = Writable({
                objectMode: true
            });

            // This `_write` method is invoked each time a new file is received
            // from the Readable stream (Upstream) which is pumping filestreams
            // into this receiver.  (filename === `__newFile.filename`).
            receiver__._write = function onFile(__newFile, encoding, done) {
                // console.log('write fd:',__newFile.fd);
                var fd = __newFile.fd;


                receiver__.once('error', function (err, db) {
                    // console.log('ERROR ON RECEIVER__ ::',err);
                    //db.close();
                    done(err);
                });


                files().create({
                    filename: fd,
                    contentType: 'binary/octet-stream',
                    length: __newFile.length,
                    chunkSize: options.maxChunkSize,
                    metadata: {
                        fd: fd,
                        dirname: __newFile.dirname || path.dirname(fd)
                    }
                }).exec(function (err, file) {
                    if (err)
                        receiver__.emit('error', err);
                    var totalBytesRead = 0;
                    var chunks = __newFile.length / options.maxChunkSize;

                    var index = 0, chunkRegistry = [];
                    while (index < chunks) {
                        chunkRegistry.push({starts: (index * options.maxChunkSize), index: index});
                        index++;
                    }
                    async.eachSeries(chunkRegistry, function (current, next) {
                        var chunkData = __newFile.read(options.maxChunkSize);
                        filesChunks().create({
                            files_id: file.id,
                            data: chunkData,
                            n: current.index
                        }).exec(function (err1, fileChunk) {
                            next(err1);
                        });

                    });
                });


            };
            return receiver__;
        }
    };

    return adapter;


    // Helper methods:
    ////////////////////////////////////////////////////////////////////////////////


};