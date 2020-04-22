var rlp = require('rlp')

exports.create = function (files) {
  var ret = [ 1 ]

  for (var key in files) {
    ret.push([ key, files[key] ])
  }

  return rlp.encode(ret)
}

exports.parse = function (manifest) {
  var ret = rlp.decode(manifest)

  if (!ret.shift().equals(Buffer.from([ 1 ]))) {
    throw new Error('Invalid manifest')
  }

  var files = {}
  for (var i = 0; i < ret.length; i++) {
    files[ ret[i][0].toString() ] = ret[i][1].toString()
  }

  return files
}
