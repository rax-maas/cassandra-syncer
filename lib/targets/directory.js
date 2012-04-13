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
var path = require('path');
var util = require('util');
var fs = require('fs');
var fsutil = require('../fsutil');
var target = require('../target');
var log = require('logmagic').local('lib.targets.directory');

var Manifest = require('../manifest').Manifest;
var Target = target.Target;
var DATA_DIR = target.DATA_DIR;
var MANIFEST_DIR = target.MANIFEST_DIR;
var ONE_MEGABYTE = 1048576;

function DirectoryTarget(url, temppath, options) {
  Target.call(this, url, temppath, options);
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

    fsutil.mkdir(path.dirname(destinationPath), 700, function(err) {
      var reader = fs.createReadStream(sourcePath, {'bufferSize': ONE_MEGABYTE}),
          writer = fs.createWriteStream(destinationPath);
      util.pump(reader, writer, callback);
    });
  });
};

DirectoryTarget.prototype._storeFile = function(file, relativeFilePath, callback) {
  var newpath = path.join(this._path, DATA_DIR, path.basename(file));
  log.msg('DirectoryTarget: copying ' + file + ' to ' + newpath);
  this._copyFile(file, newpath, callback);
};

DirectoryTarget.prototype.removeFile = function(file, callback) {
  file = path.join(this._path, DATA_DIR, path.basename(file));
  log.info('Removing file', file);
  fs.unlink(file, callback);
};

DirectoryTarget.prototype._displayName = function(file) {
  return path.join(this._path, DATA_DIR, path.basename(file));
};

DirectoryTarget.prototype._readFileStream = function(file) {
  var name = path.join(this._path, DATA_DIR, file);
  return fs.createReadStream(name);
};

DirectoryTarget.prototype._findLatestManifest = function(callback) {
  var self = this,
      manifest_dir = path.join(self._path, MANIFEST_DIR);
  fs.readdir(manifest_dir, function(err, files) {
    if (err) {
      if (err.code === 'ENOENT') {
        callback(null);
      } else {
        callback(err);
        return;
      }
    }

    if (files.length > 0) {
      files = files.filter(function(value) {
        return value.match(Manifest.regex());
      }).sort();
    }

    callback(null, files[files.length - 1]);
  });
};

DirectoryTarget.prototype.initialize = function(callback) {
  callback();
};

DirectoryTarget.prototype.readManifest = function(callback) {
  var self = this,
      file,
      o;

  async.waterfall([
    function findLatestManifest(callback) {
      self._findLatestManifest(callback);
    },

    function readFile(file, callback) {
      if (!file) {
        self.manifest = new Manifest();
        callback(null, self.manifest);
        return;
      }
      fs.readFile(path.join(self._path, MANIFEST_DIR, file), function(err, data) {
        if (err) {
          if (err.code === 'ENOENT') {
            self.manifest = new Manifest();
            callback(null, self.manifest);
          } else {
            callback(err);
          }
          return;
        }

        try {
          o = JSON.parse(data);
        } catch (e) {
          callback(e);
          return;
        }

        self.manifest = Manifest.createFromObject(o);
        callback(null, self.manifest);
      });
    }
  ], callback);
};

DirectoryTarget.prototype.writeManifest = function(callback) {
  var self = this,
      manifest_dir = path.join(self._path, MANIFEST_DIR),
      file = path.resolve(path.join(manifest_dir, Manifest.filename()));

  log.info('Writing manifest', path.basename(file));

  fsutil.mkdir(manifest_dir, 700, function(err) {
    if (err) {
      callback(err);
      return;
    }
    fs.writeFile(file, self.manifest.serialize(), callback);
  });
};

exports.target = exports.Directory = DirectoryTarget;
