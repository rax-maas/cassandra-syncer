var cloudfiles = require('cloudfiles');
var log = require('logmagic').local('lib.targets.cloudfiles');
var fs = require('fs');
var path= require('path');
var target = require('../target');
var util = require('util');

var Manifest = require('../Manifest').Manifest;

var Target = target.Target;
var DATA_DIR = target.DATA_DIR;
var MANIFEST_DIR = target.MANIFEST_DIR;
var CONTAINER = 'cassandra-backups';

function CloudfilesTarget(url, temppath) {
  var tokens;

  Target.call(this, url, temppath);

  if (!url.auth) {
    throw new Error('Auth credentials not specified');
  }

  tokens = url.auth.split(':');
  if (tokens.length != 2) {
    throw new Error('username and apiKey must be specified');
  }

  this._path = url.hostname + url.pathname;
  this.client = cloudfiles.createClient({
    auth : {
      username: tokens[0],
      apiKey: tokens[1]
    }
  });
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
  self.client.addFile(CONTAINER, {'remote': destinationPath, 'local': sourcePath}, callback);
}

CloudfilesTarget.prototype._storeFile = function(file, callback) {
  var newpath = path.join(DATA_DIR, path.basename(file));
  log.msg('CloudfilesTarget: copying ' + file + ' to ' + newpath);
  this._copyFile(file, newpath, callback);
};

CloudfilesTarget.prototype._displayName = function(file) {
  return path.join(DATA_DIR, path.basename(file));
};

CloudfilesTarget.prototype._readFileStream = function(file) {
  log.info('_readFileStream');
};

CloudfilesTarget.prototype._findLatestManifest = function(callback) {
  var self = this;
  self.client.setAuth(function(err) {
    if (err) {
      callback(err);
      return;
    }
    self.client.getContainer(CONTAINER, function(err, container) {
      if (err) {
        callback(err);
        return;
      }
      container.getFiles(function(err, files) {
        if (err) {
          callback(err);
          return;
        }

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
      });
    });
  });
};

CloudfilesTarget.prototype.removeFile = function(file, callback) {
  log.info('removeFile');
};

CloudfilesTarget.prototype.readManifest = function(callback) {
  var self = this, o;
  self._findLatestManifest(function(err, file) {
    if (err) {
      callback(err);
      return;
    }
    if (!file) {
      self.manifest = new Manifest();
      callback(null, self.manifest);
      return;
    }
    log.info('Manifest file', file);
    self.client.setAuth(function(err) {
      if (err) {
        callback(err);
        return;
      }
      self.client.getFile(CONTAINER, file, function(err, fileStream) {
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
        self.manifest = Manifest.createFromObject(o);
        callback(null, self.manifest);
      });
    });
  });
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
  log.info('writeManifest');

  var self = this;
  var file = path.join(MANIFEST_DIR, Manifest.filename());
  var localFile = path.basename(file) + randstr(8);

  log.info('Manifest URL', file);

  self.client.getContainer(CONTAINER, function(err, container) {
    if (err) {
      callback(err);
      return;
    }
    fs.writeFile(localFile, self.manifest.serialize(), function(err) {
      if (err) {
        callback(err);
        return;
      }
      self.client.addFile(CONTAINER, {'remote': file, 'local': localFile}, function(err, uploaded) {
        if (err) {
          callback(err);
          return;
        }
        fs.unlink(localFile, callback);
      });
    });
  });
};

