var async = require('async');
var fs = require('fs');
var fsutil = require('./fsutil');
var log = require('logmagic').local('lib.manifest');
var os = require('os');
var path = require('path');
var sprintf = require('sprintf').sprintf;


function Manifest(source, re) {
  this._source = source;
  this._re = re;
  this._files = [];
  this._manifest = {};
}

Manifest.prototype._generateManifest = function(callback) {
  var self = this;

  function iter(file, callback) {
    log.info('Adding file to manifest', file);
    fs.stat(path.join(self._source, file), function(err, stat) {
      if(err) {
        callback(err);
        return;
      }
      self._manifest[file] = stat.size;
      callback();
    });
  }

  async.forEach(this._files, iter, function(err) {
    if(err) {
      callback(err);
      return;
    }
    log.info('Manifest', self._manifest);
    callback();
  });
};

Manifest.prototype.generate = function(callback) {
  var self = this;
  fsutil.listFiles(this._source, /.*\.db/, function(err, files) {
    if (err) {
      callback(err);
      return;
    }
    self._files = files;
    self._generateManifest(callback);
  });
};

Manifest.prototype.filename = function() {
  return sprintf('manifest-%s.json', os.hostname());
};

Manifest.prototype.save = function(filename, callback) {
  fs.writeFile(filename, JSON.stringify(this._manifest), callback);
};

exports.Manifest = Manifest;
