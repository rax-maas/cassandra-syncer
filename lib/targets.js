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

var ONE_MEGABYTE = 1048576;


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

  console.log(file);

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

DirectoryTarget.prototype.removeFile = function(file, callback) {
  file = path.join(this._path, path.basename(file));
  log.info('Removing file', file);
  fs.unlink(file, callback);
};

DirectoryTarget.prototype._listFiles = function(re, callback) {
  fsutil.listFiles(this._path, re, callback);
};

DirectoryTarget.prototype._displayName = function (file) {
  return path.join(this._path, path.basename(file));
};

DirectoryTarget.prototype._readFileStream = function(file) {
  var name = path.join(this._path, file);
  return fs.createReadStream(name);
};

DirectoryTarget.prototype.readManifest = function(callback) {
  var self = this;
  var file;
  var o;

  file = path.resolve(path.join(this._path, Manifest.filename()));
  fs.readFile(file, function(err, data) {
    if (err) {
      if (err.code === 'ENOENT') {
        err = null;
        self.manifest = new Manifest();
      }
      callback(err);
      return;
    }

    try {
      o = JSON.parse(data);
      self.manifest = Manifest.createFromObject(o);
      callback(null, self.manifest);
    } catch (e) {
      callback(e);
      return;
    }
  });
};

DirectoryTarget.prototype.writeManifest = function(callback) {
  var file = path.resolve(path.join(this._path, Manifest.filename()));
  log.info('Saving manifest', file);
  fs.writeFile(file, this.manifest.serialize(), function(err) {
    console.log(callback);
    callback();
  });
};


exports.DirectoryTarget = DirectoryTarget;
exports.CloudfilesTarget = CloudfilesTarget;
