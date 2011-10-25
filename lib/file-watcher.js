var fs = require('fs');
var log = require('logmagic').local('lib.file-watcher');
var util = require('util');
var stalker = require('stalker');

var events = require('events');

var STEP_TIMEOUT = 3000; // 30 seconds
var MAX_TIMEOUT = 30000; // 30 seconds

function FileWatcher(directoryPath) {
  events.EventEmitter.call(this);
  this._directoryPath = directoryPath;
  this._stalker = null;
  this._watches = {};
}
util.inherits(FileWatcher, events.EventEmitter);

FileWatcher.prototype._generateTimeout = function(file) {
  var count = this._watches[file].count;
  return count > 10 ? MAX_TIMEOUT : count * STEP_TIMEOUT;
};

FileWatcher.prototype._syncCallback = function(err, file) {
  var self = this;

  if (err) {
    return;
  }

  function trigger() {
    fs.stat(file, function(err, stats) {
      var timeout;
      if (stats.size === self._watches[file].fileSize) {
        log.info('Emitting file', file);
        self.emit('file', file);
        delete self._watches[file];
      } else {
        self._watches[file].count++;
        self._watches[file].fileSize = stats.size;
        timeout = self._generateTimeout(file);
        log.info('Watching file', { file: file, timeout: timeout });
        self._watches[file].timeout = setTimeout(trigger, timeout);
      }
    });
  }

  fs.stat(file, function(err, stats) {
    if (err) {
      return;
    }
    log.info('Adding watch', {file: file, timeout: STEP_TIMEOUT});
    self._watches[file] = {
      timeout: setTimeout(trigger, STEP_TIMEOUT),
      file: file,
      count: 0,
      fileSize: stats.size
    };
  });
};

FileWatcher.prototype.run = function() {
  var self = this;
  self._stalker = stalker.watch(self._directoryPath, function(err, file) {
    console.log(file);
    self._syncCallback(err, file);
  });
};

exports.FileWatcher = FileWatcher;
