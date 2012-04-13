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

var async = require('async');
var os = require('os');
var fs = require('fs');
var util = require('util');
var url = require('url');
var path = require('path');
var mkdirp = require('mkdirp');
var sprintf = require('sprintf').sprintf;
var cloudfiles = require('cloudfiles');
var fsutil = require('./fsutil');
var Manifest = require('./manifest').Manifest;
var log = require('logmagic').local('lib.targets');

var DATA_DIR = exports.DATA_DIR = 'data';
var MANIFEST_DIR = exports.MANIFEST_DIR = 'manifest';

function Target(url, temppath, re, options) {
  this._url = url;
  this._temppath = temppath;
  this._options = options;
  this._re = re;
  this.manifest = new Manifest(null, re);
}

Target.prototype._hostPrefix = function() {
  /* TODO: configurable? */
  return os.hostname();
};

Target.prototype._filePath = function(file) {
  return path.relative(this._options.source, file);
};

Target.prototype.sync = function(file, callback) {
  var self = this,
      hardlpath,
      relativeFilePath = this._filePath(file),
      directoryPath = path.resolve(path.join(this._temppath, DATA_DIR));

  hardlpath = path.resolve(path.join(directoryPath, relativeFilePath));
  file = path.resolve(file);

  if (hardlpath === file) {
    /* skip things in my own hardlink path */
    return;
  }

  log.msg('hard linking file: '+ file + ' to '+ this._filePath(hardlpath));

  mkdirp(path.dirname(hardlpath), function(err) {
    if (err) {
      callback(err);
      return;
    }
    mkdirp(path.dirname(hardlpath), function(err) {
      if (err) {
        log.info('Error creating hardlink path', err);
        return;
      }
      fs.unlink(hardlpath, function() {
        /* ignored error */
        fs.link(file, hardlpath, function(err) {
          if (err) {
            if (err.code === "ENOENT") {
              /* maybe the sstable was deleted before we could make a hardlink */
              callback(null);
              return;
            }
            callback(err);
            return;
          }
          self._storeFile(hardlpath, relativeFilePath, function(err) {
            if (err) {
              log.info('error storing file', err);
            }
            fs.unlink(hardlpath, function() {
              callback();
            });
          });
        });
      });
    });
  });
};

Target.prototype._storeFile = function(file, relativeFilePath, callback) {
  throw new Error('you must implement this interface in your child class');
};

Target.prototype._readFileStream = function(file) {
  throw new Error('you must implement this interface in your child class');
};

Target.prototype._displayName = function (file) {
  throw new Error('you must implement this interface in your child class');
};

Target.prototype.initialize = function(callback) {
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
  var buffer = '',
      s = this._readFileStream(file);

  s.on('error', callback);
  s.on('data', function(data) {
    buffer += data;
  });
  s.on('close', function() {
    callback(null, buffer);
  });
};


Target.prototype._fetchIndex = function(file, callback) {
  var self = this,
      j;
  this._readFileWhole(file, function(err, data) {
    if (err) {
      callback(err);
      return;
    }
    try {
      j = JSON.parse(data);
    } catch(e) {
      callback(e);
      return;
    }

    j.name = self._displayName(file);

    if (j.version !== 'csync1') {
      callback(new Error('Invalid version in index '+ j.name));
      return;
    }

    callback(null, j);
  });
};

/* Fsck a single backup instance */
Target.prototype._fsckBackup = function(backup, callback) {
  log.debug('Fscking ' + backup.name + '!');
  callback();
};

/* Crawl the target, determining if the backup is available to be restored */
Target.prototype.fsck = function(callback) {
  var self = this;

  this._listBackups(function(err, backups) {
    if (err) {
      callback(err);
      return;
    }
    log.debug('Fscking '+ backups.length + ' backups');
    async.forEachSeries(backups,
      function (backup, callback) {
        self._fsckBackup(backup, function(err) {
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
  var self = this,
      backups = [];

  this._listFiles(this._re, function(err, files) {
    if (err) {
      callback(err);
      return;
    }
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

Target.prototype.restore = function(callback) {
  var self = this,
      filename,
      targetDir = path.resolve(url.parse(this._options.target).host);

  function copy(filename, callback) {
    log.info('Downloading ' + filename);
    var filePath = path.join(targetDir, filename);
    async.series([
      function createDir(callback) {
        mkdirp(path.dirname(filePath), callback);
      },
      function get(callback) {
        self._retrieveFile(filename, path.join(targetDir, filename), callback);
      }
    ], callback);
  }

  function done(err) {
    if (err) {
      log.info(sprintf('Error Downloading %s. %s', filename, err.message));
    } else {
      log.info('Finished Downloading ' + filename);
    }
  }

  async.waterfall([
    self.readManifest.bind(self),
    function queueDownloads(manifest, callback) {
      var queue = async.queue(copy, 4), filename;
      for (filename in manifest.files()) {
        queue.push(filename, done);
      }
      queue.drain = callback;
    }
  ], callback);
};

exports.Target = Target;

