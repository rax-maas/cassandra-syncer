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
var fs = require('fs');
var log = require('logmagic').local('lib.file-watcher');
var util = require('util');
var stalker = require('stalker');

var events = require('events');

/* Watches a directory for file size changes */

var STEP_TIMEOUT = 3000; //  3 seconds
var MAX_TIMEOUT = 30000; // 30 seconds

function FileWatcher(directoryPath, re) {
  events.EventEmitter.call(this);
  this._directoryPath = directoryPath;
  this._stalker = null;
  this._watches = {};
  this._re = re;
}
util.inherits(FileWatcher, events.EventEmitter);

FileWatcher.prototype._generateTimeout = function(file) {
  var count = this._watches[file].count;
  return count > 10 ? MAX_TIMEOUT : count * STEP_TIMEOUT;
};

FileWatcher.prototype._syncCallback = function(err, file) {
  var self = this;

  if (err) {
    log.info('Error', { err: err });
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
      log.info('Stat Error', { err: err });
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
    if (self._re.test(file)) {
      self._syncCallback(err, file);
    }
  });
};

exports.FileWatcher = FileWatcher;
