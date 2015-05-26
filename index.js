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
 * skipper-orientdb
 *
 * @param  {Object} globalOpts
 * @return {Object}
 */

module.exports = function OrientDBStore(globalOpts) {
    globalOpts = globalOpts || {};

    _.defaults(globalOpts, {
        maxChunkSize: 8198, //1MB
        filesCollection: 'files',
        filesChunksCollection: 'fileschunks',
        filesChunksClusterName: 'fileschunksdata'
    });

    //var getConnection = _connectionBuilder(globalOpts);

    var files = function () {
            return globalOpts.filesCollection;
        },
        filesChunks = function () {
            return globalOpts.filesChunksCollection;
        };

    var clusterReady = false,
        ensureCluster = function (callback) {

            if (clusterReady) callback();
            else {
                db.cluster.getByName(globalOpts.filesChunksClusterName, true)
                    .then(function (cluster) {
                        if (!cluster) {
                            db.cluster.create(globalOpts.filesChunksClusterName)
                                .then(function (cl) {
                                    clusterReady = true;
                                    callback();
                                });
                        } else {
                            clusterReady = true;
                            callback();
                        }
                    });
            }
        };

    var db = files().getDB();

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
        },

        readLastVersion: function (fd, cb) {
            console.log("Not implemented yet!!!");
            return;
            this.readVersion(fd, -1, cb);

        },

        readVersion: function (fd, version, cb) {
            console.log("Not implemented yet!!!");
            return;

        },

        rm: function (fd, cb) {
            console.log("Not implemented yet!!!");
        },

        /**
         * A simple receiver for Skipper that writes Upstreams to
         * orientdb
         *
         *
         * @param  {Object} options
         * @return {Stream.Writable}
         */
        receive: function OrientDBReceiver(options) {
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

                var bytesCount = parseInt(__newFile.byteCount);

                ensureCluster(function () {
                    files().create({
                        filename: fd,
                        contentType: 'binary/octet-stream',
                        length: bytesCount,
                        chunkSize: options.maxChunkSize,
                        metadata: {
                            fd: fd,
                            dirname: __newFile.dirname || path.dirname(fd)
                        }
                    }).exec(function (err, file) {
                        if (err) {
                            receiver__.emit('error', err);
                            return;
                        }
                        var totalBytesRead = 0;
                        var chunks = bytesCount / options.maxChunkSize;

                        var index = 0, chunkRegistry = [];
                        while (index < chunks) {
                            chunkRegistry.push({starts: (index * options.maxChunkSize), index: index});
                            index++;
                        }
                        var refs = [];
                        async.eachSeries(chunkRegistry, function (current, next) {
                            var bytesToRead = options.maxChunkSize;
                            if (current.index == (chunkRegistry.length - 1)) {
                                bytesToRead = bytesCount - (current.index * bytesToRead);
                            }
                            var chunkData = __newFile.read(bytesToRead);

                            files().getDB().record.create({
                                '@class': options.filesChunksClusterName,
                                data: chunkData,
                                n: current.index
                            }).then(function (fileChunk, err1) {
                                if (fileChunk) {
                                    var rid = fileChunk['@rid'];
                                    refs.push('#' + rid.cluster + ':' + rid.position);
                                }
                                next(err1);
                            });

                        }, function (err) {
                            if (err) {
                                done(err);
                            }
                            else {
                                filesChunks().create({files_id: file.id, data: refs}).then(function (fileschunk, err) {
                                    done(err);
                                });
                            }
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