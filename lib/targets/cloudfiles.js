var util = require('util');
var log = require('logmagic').local('lib.targets.cloudfiles');
var Target = require('../target').Target;
var cloudfiles = require('cloudfiles');

function CloudfilesTarget(url, temppath) {
  Target.call(this, url, temppath);
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
}

CloudfilesTarget.prototype._storeFile = function(file, callback) {
};

CloudfilesTarget.prototype._listFiles = function(re, callback) {
};

CloudfilesTarget.prototype._displayName = function (file) {
};

CloudfilesTarget.prototype._readFileStream = function(file) {
};

CloudfilesTarget.prototype._findLatestManifest = function(callback) {
};

CloudfilesTarget.prototype.removeFile = function(file, callback) {
};

CloudfilesTarget.prototype.readManifest = function(callback) {
};

CloudfilesTarget.prototype.writeManifest = function(callback) {
};

