var path = require('path');
var util = require('util');
var fs = require('fs');
var fsutil = require('../fsutil');
var Manifest = require('../Manifest').Manifest;
var target = require('../target');
var log = require('logmagic').local('lib.targets.directory');

var Target = target.Target;
var DATA_DIR = target.DATA_DIR;
var MANIFEST_DIR = target.MANIFEST_DIR;
var ONE_MEGABYTE = 1048576;

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

    fsutil.mkdir(path.dirname(destinationPath), 0700, function(err) {
      var reader = fs.createReadStream(sourcePath, {'bufferSize': ONE_MEGABYTE});
      var writer = fs.createWriteStream(destinationPath);
      util.pump(reader, writer, callback);
    });
  });
}

DirectoryTarget.prototype._storeFile = function(file, callback) {
  var newpath = path.join(this._path, DATA_DIR, path.basename(file));
  log.msg('DirectoryTarget: copying ' + file + ' to ' + newpath);
  this._copyFile(file, newpath, callback);
};

DirectoryTarget.prototype.removeFile = function(file, callback) {
  file = path.join(this._path, DATA_DIR, path.basename(file));
  log.info('Removing file', file);
  fs.unlink(file, callback);
};

DirectoryTarget.prototype._listFiles = function(re, callback) {
  fsutil.listFiles(this._path, DATA_DIR, re, callback);
};

DirectoryTarget.prototype._displayName = function (file) {
  return path.join(this._path, DATA_DIR, path.basename(file));
};

DirectoryTarget.prototype._readFileStream = function(file) {
  var name = path.join(this._path, DATA_DIR, file);
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
      } else {
        callback(err);
        return;
      }
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
};

DirectoryTarget.prototype.writeManifest = function(callback) {
  var file = path.resolve(path.join(this._path, Manifest.filename()));
  log.info('Saving manifest', file);
  fs.writeFile(file, this.manifest.serialize(), function(err) {
    callback();
  });
};

exports.target = exports.Directory = DirectoryTarget;
