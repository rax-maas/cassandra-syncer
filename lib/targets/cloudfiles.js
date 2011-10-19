var util = require('util');
var log = require('logmagic').local('lib.targets.cloudfiles');
var Target = require('../target').Target;

function CloudfilesTarget(url, temppath) {
  Target.call(this, url, temppath);
}

util.inherits(CloudfilesTarget, Target);

exports.target = exports.CloudfilesTarget = CloudfilesTarget;
