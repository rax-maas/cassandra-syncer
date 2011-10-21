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
var COLLECTION = 'cassandra-backups';

function CloudfilesTarget(url, temppath) {
  Target.call(this, url, temppath);
  this._path = url.hostname + url.pathname;
  this.client = cloudfiles.createClient({
    auth : {
      username: 'ryanphillips',
      apiKey: '0209db2d799f1f4907456ce3cfa9dd33'
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

  self.client.getContainer(COLLECTION, function(err, container) {
    if (err) {
      callback(err);
      return;
    }
    container.addFile({'remote': destinationPath, 'local': sourcePath}, callback);
  });
}

CloudfilesTarget.prototype._storeFile = function(file, callback) {
  var newpath = path.join(DATA_DIR, path.basename(file));
  log.msg('CloudfilesTarget: copying ' + file + ' to ' + newpath);
  this._copyFile(file, newpath, callback);
};

CloudfilesTarget.prototype._listFiles = function(re, callback) {
  log.info('_listFiles');
};

CloudfilesTarget.prototype._displayName = function(file) {
  return path.join(DATA_DIR, path.basename(file));
};

CloudfilesTarget.prototype._readFileStream = function(file) {
  log.info('_readFileStream');
};

CloudfilesTarget.prototype._findLatestManifest = function(callback) {
  var self = this;
  log.info('_findLatestManifest');
  self.client.setAuth(function(err) {
    if (err) {
      callback(err);
      return;
    }
    self.client.getContainer(COLLECTION, function(err, container) {
      if (err) {
        callback(err);
        return;
      }
      container.getFiles(Manifest.regex(), function(err, files) {
        log.info('Manifest Files', files);
        if (files.length == 0) {
          callback(null);
        } else {
          files = files.filter(function(value) {
            return value.match(Manifest.regex());
          }).sort();
        }
        callback(null, files[files.length - 1]);
      });
    });
  });
};

CloudfilesTarget.prototype.removeFile = function(file, callback) {
  log.info('removeFile');
};

CloudfilesTarget.prototype.readManifest = function(callback) {
  var self = this,
      o;

  log.info('readManifest');
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
    self.client.getContainer(COLLECTION, function(err, container) {
      log.info('Fetching file', file);
      if (err) {
        callback(err);
        return;
      }
      log.info('Manifest file', file);
      self.client.getFile(COLLECTION, file, function(err, data) {
        try {
          o = JSON.parse(data);
        } catch (e) {
          callback(e);
          return;
        }
      });
    });
  });
};

CloudfilesTarget.prototype.writeManifest = function(callback) {
  log.info('writeManifest');

  var self = this;
  var file = path.join(MANIFEST_DIR, Manifest.filename());
  var localFile = path.basename(file);

  log.info('Manifest URL', file);

  self.client.getContainer(COLLECTION, function(err, container) {
    if (err) {
      callback(err);
      return;
    }
    fs.writeFile(localFile, self.manifest.serialize(), function(err) {
      if (err) {
        callback(err);
        return;
      }
      container.addFile({'remote': file, 'local': localFile}, function (err, uploaded) {
        if (err) {
          callback(err);
          return;
        }
        fs.unlink(localFile, callback);
      });
    });
  });
};

