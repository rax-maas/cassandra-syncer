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
var cloudfiles = require('cloudfiles');

/* TODO: logmagic? */
var log = {'msg': function() { console.log.apply(null, arguments); },
           'err': function() { console.error.apply(null, arguments); }
           };

var ONE_MEGABYTE = 1048576;

function Target(url, temppath) {
  this._temppath = temppath;
}

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

function CloudfilesTarget(url, temppath) {
  Target.call(this, url, temppath);
}

util.inherits(CloudfilesTarget, Target);

function DirectoryTarget(url, temppath) {
  Target.call(this, url, temppath);
  this._path = url.hostname + url.pathname;
  log.msg('DirectoryTarget syncing to '+ this._path);
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



exports.DirectoryTarget = DirectoryTarget;
exports.CloudfilesTarget = CloudfilesTarget;
