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

var os = require('os');
var fs = require('fs');
var util = require('util');
var url = require('url');
var path = require('path');
var async = require('async');
var cloudfiles = require('cloudfiles');
var fsutil = require('./fsutil');


var Manifest = require('./manifest').Manifest;

var log = require('logmagic').local('lib.targets');


function Target(url, temppath, re) {
  this._url = url;
  this._temppath = temppath;
  this._re = re;
  this.manifest = new Manifest(null, re);
}

Target.prototype._hostPrefix = function() {
  /* TODO: configurable? */
  return os.hostname();
};

Target.prototype.sync = function(file, callback) {
  var self = this;
  var hardlpath = path.resolve(path.join(this._temppath, path.basename(file)));
  file = path.resolve(file);

  if (hardlpath == file) {
    /* skip things in my own hardlink path */
    return;
  }

  log.msg('hard linking file: '+ file + ' to '+ hardlpath);

  fs.unlink(hardlpath, function() {
    /* ignored error */
    fs.link(file, hardlpath, function(err) {
      if (err && err.code == "ENOENT") {
        /* maybe the sstable was deleted before we could make a hardlink */
        callback(null);
        return;
      }
      if (err) {
        callback(err);
        return;
      }

      self._storeFile(hardlpath, function(err) {
        fs.unlink(hardlpath, function() {
          callback(err);
        });
      });
    });
  })
};

Target.prototype._storeFile = function(file, callback) {
  throw new Error('you must implement this interface in your child class');
};

Target.prototype._listFiles = function(callback) {
  throw new Error('you must implement this interface in your child class');
};

Target.prototype._readFileStream = function(file) {
  throw new Error('you must implement this interface in your child class');
};

Target.prototype._displayName = function (file) {
  throw new Error('you must implement this interface in your child class');
};

Target.prototype.removeFile = function (file, callback) {
  throw new Error('you must implement this interface in your child class');
};

Target.prototype.readManifest = function(callback) {
  throw new Error('you must implement this interface in your child class');
};

Target.prototype.writeManifest = function(callback) {
  throw new Error('you must implement this interface in your child class');
};

Target.prototype._readFileWhole = function(file, callback) {
  var buffer = '';

  var s = this._readFileStream(file);

  s.on('error', callback);
  s.on('data', function(data) {
    buffer += data;
  });
  s.on('close', function() {
    callback(null, buffer);
  });
};


Target.prototype._fetchIndex = function(file, callback) {
  var self = this;
  this._readFileWhole(file, function(err, data) {
    if (err) {
      callback(err);
      return;
    }
    try {
      var j = JSON.parse(data);
    }
    catch (err) {
      callback(err);
      return;
    }

    j.name = self._displayName(file);

    if (j.version != 'csync1') {
      callback(new Error('Invalid version in index '+ j.name));
      return;
    }

    callback(null, j);
  });
}

/* Fsck a single backup instance */
Target.prototype._fsckBackup = function(logger, backup, callback) {
  logger('Fscking ' + backup.name + '!');
  callback();
};

/* Crawl the target, determining if the backup is available to be restored */
Target.prototype.fsck = function(logger, callback) {
  var self = this;

  this._listBackups(function(err, backups) {
    if (err) {
      callback(err);
      return;
    }
    logger('Fscking '+ backups.length + ' backups');
    async.forEachSeries(backups,
      function (backup, callback) {
        self._fsckBackup(logger, backup, function(err) {
          if (err) {
            callback(err);
            return;
          }
          callback();
        });
      },
      function (err) {
        callback(err);
      });
  });
};

Target.prototype._listBackups = function(callback) {
  var self = this;
  var backups = [];

  this._listFiles(this._re, function(err, files) {
    async.forEachSeries(files,
      function (file, callback) {
        self._fetchIndex(file, function(err, index) {
          if (err) {
            callback(err);
            return;
          }
          backups.push(index);
          callback();
        });
      },
      function (err) {
        callback(err, backups);
      });
  });
};

exports.Target = Target;

