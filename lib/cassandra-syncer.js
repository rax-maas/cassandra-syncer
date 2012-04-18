/*
 *  Copyright 2011 Rackspace
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

var fs = require('fs');
var util = require('util');
var url = require('url');
var path = require('path');
var sprintf = require('sprintf').sprintf;

var async = require('async');

var mkdir = require('./fsutil').mkdir;
var targets = require('./targets');

var FileWatcher = require('./file-watcher').FileWatcher;
var Manifest = require('./manifest').Manifest;

var log = require('logmagic').local('lib.cassandra-syncer');

function SyncMaster(options, callback) {
  var self = this;
  this._options = options;
  this._target = null;
  this._watcher = null;
  this._callback = null;
  this._backupDir = null;
  this._re = options.re;
  this._endBack = callback;
  this._pendingUploads = {};
  this._callback = function(err) {
    self._endBack(err);
  };
  this.manifest = null;
}


SyncMaster.prototype._writeManifest = function() {
  var self = this;
  this._target.manifest.setManifest(this.manifest);
  this._target.writeManifest(function(err) {
    if (err) {
      log.info('Error writing manifest', err);
      return;
    }
    log.info('Wrote manifest', self._target.manifest.serialize());
  });
};


SyncMaster.prototype._addFileToPendingUploads = function(file) {
  this._pendingUploads[file] = 1;
};

SyncMaster.prototype._removeFileFromPendingUploads = function(file) {
  delete this._pendingUploads[file];
};

SyncMaster.prototype._arePendingUploadsDone = function() {
  return Object.keys(this._pendingUploads).length === 0;
};

SyncMaster.prototype._syncCallback = function(file) {
  var self = this;
  self._addFileToPendingUploads(file);
  self._target.sync(file, function(err) {
    if (err) {
      log.info(sprintf('Error syncing (file=%s, err=%s)', file, err.message));
      self._removeFileFromPendingUploads(file);
      return;
    }

    // add file to manifest
    self.manifest.add(file);

    // remove file from pending uploads
    self._removeFileFromPendingUploads(file);
    if (self._arePendingUploadsDone()) {
      self._writeManifest();
    }
  });
};

SyncMaster.prototype._validate = function(callback) {
  var self = this,
      q,
      manifestFileSize;

  function validate(manifest) {
    return function(filename, callback) {
      var filePath = self._target.getLocalFilePath(filename);
      log.debug(sprintf('Validating %s %s', filename, filePath));
      async.waterfall([
        fs.stat.bind(null, filePath),
        function(stats, callback) {
          manifestFileSize = manifest.get(filename);
          if (manifestFileSize !== stats.size) {
            log.error(sprintf('manifest mismatch for file %s', filename));
            self._syncCallback(self._fw.getLocalPath(filename));
            callback();
          } else {
            self._target.getStats(filename, function(err, cloudfile) {
              if (err) {
                log.info(sprintf('failed retriving file %s within manifest (cassandra compaction?)', filename));
              } else {
                if (cloudfile.bytes != stats.size) {
                  log.info(sprintf('%s local filesize differs from cloud filesize (%s !== %s)',
                                   filename, stats.size, cloudfile.bytes));
                  self._syncCallback(self._fw.getLocalPath(filename));
                } else {
                  log.info(sprintf('%s validated ok', filename));
                }
              }
              callback();
            });
          }
        }
      ], callback);
    };
  }

  log.debug('Beginning validation');
  async.waterfall([
    self._target.readManifest.bind(self._target),
    function(manifest, callback) {
      q = async.queue(validate(manifest), 4);
      q.push(Object.keys(manifest.files()));
      q.drain = callback;
    }
  ], callback);
};

SyncMaster.prototype.run = function(callback) {
  var target = url.parse(this._options.target),
      T = targets[target.protocol.replace(':', '')],
      self = this;

  if (!T) {
    callback(new Error("Invalid Target type"));
    return;
  }

  if (this._options.backupDirectory) {
    this._backupDir = this._options.backupDirectory;
  }
  else {
    this._backupDir = path.join(this._options.source, 'snapshots/_syncer');
  }

  log.msg('Hard linking into ' + this._backupDir);
  this.manifest = new Manifest(this._options.source, this._re);
  this._target = new T(target, this._backupDir, this._re, this._options);
  this._target.initialize(function(err) {
    if (err) {
      callback(err);
      return;
    }
    self._fw = new FileWatcher(self._options.source, self._re);
    self._fw.on('file', self._syncCallback.bind(self));
    self._fw.run();
  });

  function registerValidation() {
    var initialTimeout = 17 * 60 * 1000,
        jitter = Math.random() * 17 * 60 * 1000,
        timeout = initialTimeout + jitter;

    log.msg(sprintf('Registering validation in %s ms', timeout));

    setTimeout(function() {
      self._validate(function(err) {
        if (err) {
          log.error('validation error', {err: err});
        }
        registerValidation();
      });
    }, timeout);
  }

  registerValidation();
};

function sync(options, callback) {
  var sm = new SyncMaster(options);
  sm.run(callback);
}

/** Fsck Master **/

function FsckMaster(options) {
  this._options = options;
}

FsckMaster.prototype.run = function(callback) {
  var target = url.parse(this._options.target),
      T = targets[target.protocol.replace(':', '')];

  if (!T) {
    callback(new Error("Invalid Target type"));
    return;
  }

  this._target = new T(target);
  this._target.fsck(callback);
};

function fsck(options, callback) {
  var sm = new FsckMaster(options);
  sm.run(callback);
}

/** Restore Master **/

function RestoreMaster(options) {
  this._options = options;
}

RestoreMaster.prototype.run = function(callback) {
  var source = url.parse(this._options.source),
      T = targets[source.protocol.replace(':', '')];

  if (!T) {
    callback(new Error("Invalid Target type"));
    return;
  }

  this._source = new T(source, null, null, this._options);
  this._source.restore(callback);
};

function restore(options, callback) {
  new RestoreMaster(options).run(callback);
}

exports.sync = sync;
exports.fsck = fsck;
exports.restore = restore;
