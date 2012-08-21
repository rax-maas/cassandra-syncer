var spawn = require('child_process').spawn

var trim = function(str) {
  return str.replace(/^(\s*)((\S+\s*?)*)(\s*)$/, '$2');
};

function CassandraTool(options) {
  this.options = options || {};
  this.host = this.options.host || 'localhost';
  this.port = this.options.port || 9081;
  this.path = this.options.path || '/opt/cassandra/bin/nodetool'
}

CassandraTool.prototype._generateArgs = function(args) {
  return ['-h', this.host, '-p', this.port].concat(args);
};

CassandraTool.prototype.run = function(args, callback) {
  var proc = spawn(this.path, args),
      stdout = '',
      stderr = '';

  proc.stdout.on('data', function(data) {
    stdout += data;
  });
  proc.stderr.on('data', function(data) {
    stderr += data;
  });
  proc.on('exit', function(code) {
    callback(null, code, stdout, stderr);
  });

  return proc;
};

CassandraTool.prototype.snapshot = function(keyspaces, name, callback) {
  var args = ['snapshot'];

  if (typeof(keyspaces) === 'object') {
    args = args.concat(keyspaces);
  } else {
    args = args.concat([keyspaces]);
  }

  if (name) {
    args = args.concat(['-t', name]);
  }

  return this.run(this._generateArgs(args), callback);
};

// callback(err, stdout, stderr)
CassandraTool.prototype.clearsnapshot = function(keyspaces, name, callback) {
  var args = ['clearsnapshot'];

  if (typeof(keyspaces) === 'object') {
    args = args.concat(keyspaces);
  } else {
    args = args.concat([keyspaces]);
  }

  if (name) {
    args = args.concat(['-t', name]);
  }

  return this.run(this._generateArgs(args), callback);
};

// callback.(err, headers, data)
CassandraTool.prototype.ring = function(callback) {
  return this.run(this._generateArgs(['ring']), function(err, stdout, stderr) {
    if (err) {
      callback(err);
      return
    }
    var lines = stderr.split('\n');
    lines = lines.filter(function(line) {
      return line !== '';
    });
    lines = lines.map(function(line) {
      return trim(line).split(/\s+/);
    });

    callback(null, lines[0] /* headers */, lines.slice(1) /* data */);
  });
};

exports.CassandraTool = CassandraTool
