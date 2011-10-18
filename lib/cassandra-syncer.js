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
var tmod = require('./targets');

var Manifest = require('./manifest').Manifest;

var log = require('logmagic').local('lib.cassandra-syncer');

var targets = {
  'cloudfiles:': tmod.CloudfilesTarget,
  'directory:': tmod.DirectoryTarget,
}


function SyncMaster(options, callback) {
  this._options = options;
  this._target = null;
  this._watcher = null;
  this._callback = null;
  this._backupDir = null;
  this._re = /(.*-Data.*)|(^manifest-\s+.json)/;
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
    this._callback(err);
    return;
  }

  if (this._isSyncable(file)) {
    this._target.sync(file, function(err) {
      if (err) {
        self._callback(err);
        return;
      }
    });
  }
};

SyncMaster.prototype.run = function() {
  var target = url.parse(this._options.target);
  var T = targets[target.protocol];
  var manifest;
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

  async.waterfall([
    function generateLocalManifest(callback) {
      manifest = new Manifest(self._options.source, self._re);
      manifest.generate(function(err) {
        manifest.save(path.join(self._backupDir, manifest.filename()), callback);
      });
    },

    mkdir.bind(null, self._backupDir),

    function run(callback) {
      log.msg('Hard linking into ' + self._backupDir);
      self._target = new T(target, self._backupDir, self._re);
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
