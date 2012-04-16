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
var cloudfiles = require('cloudfiles');
var log = require('logmagic').local('lib.targets.cloudfiles');
var fs = require('fs');
var path= require('path');
var sprintf = require('sprintf').sprintf;
var target = require('../target');
var util = require('util');
var os = require('os');

var Manifest = require('../manifest').Manifest;

var Target = target.Target;
var DATA_DIR = target.DATA_DIR;
var MANIFEST_DIR = target.MANIFEST_DIR;

function CloudfilesTarget(url, temppath, re, options) {
  var opts;

  Target.call(this, url, temppath, this._re, options);

  if (!options.config.RACKSPACE_USERNAME) {
    throw new Error('RACKSPACE_USERNAME must be specified');
  }

  if (!options.config.RACKSPACE_TOKEN) {
    throw new Error('RACKSPACE_TOKEN must be specified');
  }

  opts = {};
  opts.auth = {};
  opts.auth.username = options.config.RACKSPACE_USERNAME;
  opts.auth.apiKey = options.config.RACKSPACE_TOKEN;
  if (options.config.USE_SERVICENET === true) {
    opts.servicenet = true;
  }
  if (options.config.HOST) {
    opts.auth.host = options.config.HOST;
  }

  this._bucket = options.bucket || os.hostname();
  this.client = cloudfiles.createClient(opts);
  if (options.config.CACHE_PATH) {
    this.client.config.cache.path = options.config.CACHE_PATH;
  }
}

util.inherits(CloudfilesTarget, Target);

exports.target = exports.CloudfilesTarget = CloudfilesTarget;

/**
* Copy a file.
*
* @param {String} sourcePath Source path.
* @param {String} destinationPath Destination path.
* @param {Function} callback Callback which is called with a possible error.
*/
CloudfilesTarget.prototype._copyFile = function (sourcePath, destinationPath, callback) {
  var self = this;
  self.client.setAuth(function() {
    self.client.addFile(self._bucket, {'remote': destinationPath, 'local': sourcePath}, callback);
  });
};

CloudfilesTarget.prototype._storeFile = function(file, relativeFilePath, callback) {
  var newpath = path.join(DATA_DIR, relativeFilePath);
  log.debug('CloudfilesTarget: copying ' + this._filePath(file) + ' to ' + newpath);
  this._copyFile(file, newpath, callback);
};

CloudfilesTarget.prototype._retrieveFile = function(file, relativeFilePath, callback) {
  file = path.join(DATA_DIR, file);
  log.debug('CloudfilesTarget: retrieving ' + file + ' to ' + relativeFilePath);
  this._getFile(file, relativeFilePath, callback);
};

CloudfilesTarget.prototype._getFile = function(filename, newFilePath, callback) {
  var self = this;
  async.waterfall([
    function(callback) {
      self.client.setAuth(function(err) {
        callback(err);
      });
    },
    function(callback) {
      self.client.getFile(self._bucket, filename, callback);
    },
    function(file, callback) {
      file.save({ local: newFilePath }, callback);
    }
  ], callback);
};

CloudfilesTarget.prototype._displayName = function(file) {
  return path.join(DATA_DIR, this._filePath(file));
};

CloudfilesTarget.prototype._readFileStream = function(file) {
  log.info('_readFileStream');
};

CloudfilesTarget.prototype._findLatestManifest = function(callback) {
  var self = this;
  async.waterfall([
    function(callback) {
      self.client.setAuth(function(err) {
        callback(err);
      });
    },
    function(callback) {
      self.client.getFiles(self._bucket, callback);
    },
    function(files, callback) {
      if (files.length === 0) {
        callback();
        return;
      }

      files = files.map(function(file) {
        return file.name;
      });

      files = files.filter(function(value) {
        return value.match(Manifest.regex());
      }).sort();

      callback(null, files[files.length - 1]);
    }
  ], callback);
};

CloudfilesTarget.prototype.initialize = function(callback) {
  var self = this;
  async.waterfall([
    function(callback) {
      self.client.setAuth(function(err) {
        callback(err);
      });
    },
    function(callback) {
      self.client.createContainer(self._bucket, callback);
    }
  ], callback);
};

CloudfilesTarget.prototype.removeFile = function(file, callback) {
  log.info('removeFile');
};

CloudfilesTarget.prototype.readManifest = function(callback) {
  var self = this, o;

  async.waterfall([
    function(callback) {
      self._findLatestManifest(callback);
    },
    function(file, callback) {
      if (!file) {
        self.manifest = new Manifest();
        callback(null, self.manifest, file);
      }
      log.info(sprintf('Manifest file %s', file));
      callback(null, null, file);
    },
    function(manifest, file, callback) {
      if (manifest) {
        callback();
      } else {
        self.client.setAuth(function(err) {
          if (err) {
            callback(err);
            return;
          }
          self.client.getFile(self._bucket, file, function(err, fileStream) {
            if (err) {
              callback(err);
              return;
            }
            var data = fs.readFileSync(fileStream.local);
            try {
              o = JSON.parse(data);
            } catch (e) {
              callback(e);
              return;
            }
            callback(null, Manifest.createFromObject(o));
          });
        });
      }
    }
  ], callback);
};

var getRandomInt = function(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
};

var randstr = function(len) {
  var chars, r, x;

  chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  r = [];

  for (x = 0; x < len; x++) {
    r.push(chars[getRandomInt(0, chars.length-1)]);
  }

  return r.join('');
};

CloudfilesTarget.prototype.writeManifest = function(callback) {
  var self = this,
      file = path.join(MANIFEST_DIR, Manifest.filename()),
      localFile = path.basename(file) + randstr(8);

  log.debug(sprintf('writeManifest %s', file));

  async.series([
    function(callback) {
      fs.writeFile(localFile, self.manifest.serialize(), callback);
    },
    function(callback) {
      self.client.setAuth(function(err) {
        callback(err);
      });
    },
    function(callback) {
      self.client.addFile(self._bucket, {'remote': file, 'local': localFile}, callback);
    },
    function(callback) {
      fs.unlink(localFile, callback);
    }
  ], callback);
};

