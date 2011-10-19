var targets = [
  'cloudfiles',
  'directory'
];


targets.forEach(function(target) {
  exports[target] = require('./' + target).target;
  exports[target + ':'] = require('./' + target).target;
});


