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
var util = require('util');
var path = require('path');

/* utilities */

/**
 * Recursively create a directory.
 *
 * @param {String} ensure  The path to recursively create.
 * @param {number} chmod  File permissions to use when creating directories.
 * @param {Function} callback     Callback, taking a possible error.
 */
function mkdir(ensure, chmod, callback) {
  if (ensure.charAt(0) !== '/') {
    ensure = path.join(process.cwd(), ensure);
  }

  var dirs = ensure.split('/');
  var walker = [];

  if (arguments.length < 3) {
    callback = chmod;
    chmod = 0755;
  }

  // gobble the "/" first
  walker.push(dirs.shift());

  (function S(d) {

    if (d === undefined) {
      callback();
      return;
    }

    walker.push(d);
    var dir = walker.join('/');

    fs.stat(dir, function(er, s) {

      if (er) {
        fs.mkdir(dir, chmod, function(er, s) {
          if (er && er.message.indexOf('EEXIST') === 0) {
            // When multiple concurrent actors are trying to ensure the same directories,
            // it can sometimes happen that something doesn't exist when you do the stat,
            // and then DOES exist when you try to mkdir.  In this case, just go back to
            // the stat to make sure it's a dir and not a file.
            S('');
            return;
          }
          else if (er) {
            callback(new Error('Failed to make ' + dir + ' while ensuring ' + ensure + '\n' + er.message));
            return;
          }
          S(dirs.shift());
        });
      }
      else {
        if (s.isDirectory()) {
          S(dirs.shift());
        }
        else {
          callback(new Error('Failed to mkdir ' + dir + ': File exists'));
        }
      }
    });
  })(dirs.shift());
}


function listFiles(path, re, callback) {
  fs.readdir(path, function(err, files) {
    if (err) {
      callback(err);
      return;
    }

    var rv = files.filter(function(file) {
      return file.match(re) !== null;
    });

    callback(null, rv);
  });
};


exports.listFiles = listFiles;
exports.mkdir = mkdir;
