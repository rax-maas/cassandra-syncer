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

var async = require('async');

var mkdir = require('./fsutil').mkdir;
var stalker = require('stalker');
var targets = require('./targets');

var Manifest = require('./manifest').Manifest;

var log = require('logmagic').local('lib.cassandra-syncer');

function SyncMaster(options, callback) {
  this._options = options;
  this._target = null;
  this._watcher = null;
  this._callback = null;
  this._backupDir = null;
  this._re = /.*-Data.db/;
  this._manifest = null;
  this._endBack = null;
  this._endBack = callback;
  var self = this;
  this._callback = function(err) {
    if (self._watcher) {
      /* TODO: support cancelling a watcher */
      // self._watcher.reset();
    }
    self._endBack(err);
  };
}

SyncMaster.prototype._isSyncable = function(file) {
  return file.match(this._re);
}

SyncMaster.prototype._syncCallback = function(err, file) {
  var self = this;

  if (err) {
    self._callback(err);
    return;
  }

  if (!self._isSyncable(file)) {
    return;
  }

  async.series([
    function(callback) {
      self._manifest.generate(callback);
    },
    function(callback) {
      self._target.manifest.setManifest(self._manifest);
      callback();
    },
    function(callback) {
      self._target.writeManifest(callback);
    },
    function(callback) {
      self._target.sync(file, callback);
    }
  ], function(err) {
    if (err) {
      process.stderr.write(err);
      self._callback(err);
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
  self._target = new T(target, self._backupDir, self._re);

  async.series([
    function generateLocalManifest(callback) {
      self._manifest = new Manifest(self._options.source, self._re);
      self._manifest.generate(callback);
    },

    function readTargetManifest(callback) {
      self._target.readManifest(callback);
    },

    function writeNewManifest(callback) {
      self._target.manifest.setManifest(self._manifest);
      self._target.writeManifest(callback);
    },

    function watch(callback) {
      self._watcher = stalker.watch(self._options.source, function(err, file) {
        self._syncCallback(err, file);
      });
      callback();
    }

  ], function(err) {
    if (err) {
      process.stderr.write(err);
    }
  });
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
