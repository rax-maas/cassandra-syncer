var fsutil = require('./fsutil');
var async = require('async');
var fs = require('fs');
var path = require('path');
var log = require('logmagic').local('lib.manifest');


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

exports.Manifest = Manifest;
