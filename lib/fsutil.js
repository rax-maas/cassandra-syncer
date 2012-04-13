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

/** List the files within a directory
 * @param {String} path The path.
 * @param {?Regex} re The filter on the files.
 * @param {Function} callback Callback, taking a possible error.
 */
function listFiles(path, re, callback) {
  fs.readdir(path, function(err, files) {
    if (err) {
      callback(err);
      return;
    }

    if (re) {
      files = files.filter(function(file) {
        return file.match(re) !== null;
      });
    }

    callback(null, files);
  });
}


exports.listFiles = listFiles;
