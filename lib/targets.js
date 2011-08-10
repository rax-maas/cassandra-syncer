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

/* TODO: logmagic? */
var log = {'msg': function() { console.log.apply(null, arguments); },
           'err': function() { console.error.apply(null, arguments); }
           };

var ONE_MEGABYTE = 1048576;
var INDEX_FILES_RE = /^cass-sync-index-\d+.json$/;

function Target(url, temppath) {
  this._temppath = temppath;
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

Target.prototype._readFileWhole = function(file, callback) {
  var buffer = '';

  var s = this._readFileStream(file);

  s.on('error', callback);
  s.on('data', function(data) {
    buffer += data;
  });
  s.on('end', function() {
    callback(null, buffer);
  })
};

Target.prototype._fetchIndex = function(file, callback) {
  console.log('fetching index '+ file);
  this._readFileWhole(file, function(err, data) {
    if (err) {
      callback(err);
      return;
    }
  });
}

/* Fsck a single backup instance */
Target.prototype._fsckBackup = function(logger, backup, callback) {
  logger('Fscking ' + backup + '!');
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
    backups.forEach(function(backup) {
      /* TODO: async.serial? */
      self._fsckBackup(logger, backup, function(err) {
        if (err) {
          callback(err);
          return;
        }
      });
    });
  });
};

Target.prototype._listBackups = function(callback) {
  var self = this;
  var backups = [];

  this._listFiles(INDEX_FILES_RE, function(err, files) {
    async.whilst(
      function () { return files.length > 0; },
      function (callback) {
        var file = files.pop();
        self._fetchIndex(file, function(err, index) {
          if (err) {
            callback(err);
            return;
          }
          backups.push(index);
        });
      },
      function (err) {
        callback(err, backups);
      })
  });
};


function CloudfilesTarget(url, temppath) {
  Target.call(this, url, temppath);
}

util.inherits(CloudfilesTarget, Target);

function DirectoryTarget(url, temppath) {
  Target.call(this, url, temppath);
  this._path = url.hostname + url.pathname;
}
util.inherits(DirectoryTarget, Target);

/**
* Copy a file.
*
* @param {String} sourcePath Source path.
* @param {String} destinationPath Destination path.
* @param {Function} callback Callback which is called with a possible error.
*/
DirectoryTarget.prototype._copyFile = function (sourcePath, destinationPath, callback) {
  fs.stat(sourcePath, function(err, stats) {
    if (err) {
      callback(err);
      return;
    }

    if (stats.isDirectory()) {
      callback(new Error('Source path must be a file'));
      return;
    }

    var reader = fs.createReadStream(sourcePath, {'bufferSize': ONE_MEGABYTE});
    var writer = fs.createWriteStream(destinationPath);

    util.pump(reader, writer, callback);
  });
}

DirectoryTarget.prototype._storeFile = function(file, callback) {
  var newpath = path.join(this._path, path.basename(file));
  log.msg('DirectoryTarget: copying ' + file + ' to ' + newpath);
  this._copyFile(file, newpath, callback);
};

DirectoryTarget.prototype._listFiles = function(re, callback) {
  fs.readdir(this._path, function(err, files) {
    if (err) {
      callback(err);
      return;
    }

    var rv = files.filter(function(file) {
      return file.match(re) !== null;
    });

    callback(null, rv);
    return;
  });
};

Target.prototype._readFileStream = function(file) {
  var name = path.join(this._path, file);
  return fs.createReadStream(name);
};

exports.DirectoryTarget = DirectoryTarget;
exports.CloudfilesTarget = CloudfilesTarget;
