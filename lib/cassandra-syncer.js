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
  this.manifest = null;
  this._pendingUploads = {};
  this._callback = function(err) {
    if (self._watcher) {
      /* TODO: support cancelling a watcher */
      // self._watcher.reset();
    }
    self._endBack(err);
  };
}


SyncMaster.prototype._writeManifest = function() {
  var self = this;
  self._target.manifest.setManifest(self.manifest);
  self._target.writeManifest(function(err) {
    if (err) {
      log.info('Error writing manifest', err);
      return;
    }
    log.info('Wrote manifest', self._target.manifest);
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
      log.info('Error syncing file', file);
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

SyncMaster.prototype.run = function() {
  var target = url.parse(this._options.target);
  var T = targets[target.protocol];
  var self = this;

  if (!T) {
    self._callback(new Error("Invalid Target type"));
    return;
  }

  if (this._options.backupDirectory) {
    this._backupDir = this._options.backupDirectory;
  }
  else {
    this._backupDir = path.join(this._options.source, 'snapshots/_syncer');
  }

  log.msg('Hard linking into ' + self._backupDir);
  self.manifest = new Manifest(self._options.source, self._re);
  self._target = new T(target, self._backupDir, self._re);
  self._fw = new FileWatcher(self._options.source, self._re);
  self._fw.on('file', function(file) {
    self._syncCallback(file);
  });
  self._fw.run();
};

function sync(options, callback) {
  var sm = new SyncMaster(options, callback);
  sm.run();
}

function FsckMaster(options, callback) {
  this._options = options;
  this._target = null;
  this._callback = callback;
}

FsckMaster.prototype.run = function() {
  var target = url.parse(this._options.target);
  var T = targets[target.protocol];

  if (!T) {
    callback(new Error("Invalid Target type"));
    return;
  }

  this._target = new T(target, null);
  /* TODO: improve */
  this._target.fsck(console.log, this._callback);
};

function fsck(options, callback) {
  var sm = new FsckMaster(options, callback);
  sm.run();
}

exports.sync = sync;
exports.fsck = fsck;
