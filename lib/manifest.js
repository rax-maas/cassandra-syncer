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
var fs = require('fs');
var fsutil = require('./fsutil');
var log = require('logmagic').local('lib.manifest');
var os = require('os');
var path = require('path');
var sprintf = require('sprintf').sprintf;


/** A Manifest Object
 * @param {String} source The Source of the manifest.
 */
function Manifest(source) {
  this._source = source;
  this._initManifest();
}


/** Initialize the manifest
 */
Manifest.prototype._initManifest = function() {
  this._manifest = {
    version: '1.0',
    timestamp: new Date().toString(),
    files: {}
  };
};

Manifest.regex = function() {
  return /.*manifest-.*.json/;
};

Manifest.createDelta = function(manifestA, manifestB) {
  var m = new Manifest(), aKeys = [];

  Object.keys(manifestA.files()).forEach(function(file) {
    if (!manifestB.contains(file)) {
      aKeys.push(file);
    }
  });

  aKeys.forEach(function(file) {
    m.add(file, manifestA.get(file));
  });

  return m;
};

Manifest.createFromObject = function(manifest) {
  var m = new Manifest();
  m._manifest = manifest;
  return m;
};

Manifest.filename = function() {
  var now = new Date();
  return sprintf('manifest-%04d%02d%02d%02d%02d%02d.json',
                 now.getFullYear(),
                 now.getMonth(),
                 now.getDate(),
                 now.getHours(),
                 now.getMinutes(),
                 now.getMinutes(),
                 now.getSeconds());
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
      self.add(file, stat.size);
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

Manifest.prototype.files = function() {
  return this._manifest.files;
};

Manifest.prototype.get = function(file) {
  return this._manifest.files[path.basename(file)];
};

Manifest.prototype.add = function(file, size) {
  if (!size) {
    stats = fs.statSync(file);
    size = stats.size;
  }
  this._manifest.files[path.basename(file)] = size;
};

Manifest.prototype.generate = function(callback) {
  this._generateManifest(this.files(), callback);
};

Manifest.prototype.contains = function(file) {
  return this._manifest.files[path.basename(file)] !== undefined;
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
