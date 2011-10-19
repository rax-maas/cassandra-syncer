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
  this._initManifest();
}

Manifest.prototype._initManifest = function() {
  this._manifest = {
    timestamp: new Date().toString(),
    files: {}
  };
};

Manifest.createFromObject = function(manifest) {
  var m = new Manifest();
  m._manifest = manifest;
  return m;
};

Manifest.filename = function() {
  return 'manifest.json';
};

Manifest.prototype.serialize = function() {
  return JSON.stringify(this._manifest);
};

Manifest.prototype._generateManifest = function(files, callback) {
  var self = this;

  self._initManifest();

  function iter(file, callback) {
    log.debug('Adding file to manifest', file);
    fs.stat(path.join(self._source, file), function(err, stat) {
      if(err) {
        callback(err);
        return;
      }
      self._manifest.files[file] = stat.size;
      callback();
    });
  }

  async.map(files, iter, function(err) {
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
  fsutil.listFiles(this._source, this._re, function(err, files) {
    if (err) {
      callback(err);
      return;
    }
    self._generateManifest(files, callback);
  });
};

Manifest.prototype.remove = function(file) {
  delete this._manifest.files[file];
};

Manifest.prototype.setManifest = function(manifest) {
  this._manifest = manifest._manifest;
};

Manifest.prototype.save = function(filename, callback) {
  fs.writeFile(filename, this.serialize(), callback);
};

exports.Manifest = Manifest;
